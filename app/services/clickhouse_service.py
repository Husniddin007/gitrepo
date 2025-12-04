# services/clickhouse_service.py

from clickhouse_driver import Client
from django.conf import settings
from typing import List, Dict
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class ClickHouseService:
    def __init__(self):
        self.client = Client(
            host=settings.CLICKHOUSE_SETTINGS['host'],
            port=settings.CLICKHOUSE_SETTINGS['port']
        )
    
    def create_database_and_table(self):
        """Database va tablelarni yaratish"""
        # Database yaratish
        self.client.execute('CREATE DATABASE IF NOT EXISTS github_analytics')
        logger.info("✅ Database yaratildi: github_analytics")
        
        # Asosiy repositories table
        self.client.execute('''
            CREATE TABLE IF NOT EXISTS github_analytics.repositories (
                owner String,
                name String,
                name_with_owner String,
                description String,
                stars UInt32,
                forks UInt32,
                watchers UInt32,
                is_fork UInt8,
                is_archived UInt8,
                language_count UInt16,
                topic_count UInt16,
                disk_usage_kb UInt64,
                pull_requests UInt32,
                issues UInt32,
                primary_language String,
                created_at DateTime,
                pushed_at DateTime,
                created_year UInt16,
                created_date Date,
                default_branch_commit_count UInt32,
                license String,
                assignable_user_count UInt16,
                code_of_conduct String,
                forking_allowed UInt8,
                has_parent UInt8
            ) ENGINE = MergeTree()
            ORDER BY (created_year, stars, name_with_owner)
            SETTINGS index_granularity = 8192
        ''')
        logger.info("✅ Table yaratildi: repositories")
        
        # Languages table
        self.client.execute('''
            CREATE TABLE IF NOT EXISTS github_analytics.repository_languages (
                repo_name_with_owner String,
                language String,
                size UInt64,
                created_year UInt16,
                repo_stars UInt32,
                repo_forks UInt32
            ) ENGINE = MergeTree()
            ORDER BY (created_year, language, size)
            SETTINGS index_granularity = 8192
        ''')
        logger.info("✅ Table yaratildi: repository_languages")
        
        # Topics table
        self.client.execute('''
            CREATE TABLE IF NOT EXISTS github_analytics.repository_topics (
                repo_name_with_owner String,
                topic String,
                topic_stars UInt32,
                created_year UInt16,
                repo_stars UInt32
            ) ENGINE = MergeTree()
            ORDER BY (topic, created_year, topic_stars)
            SETTINGS index_granularity = 8192
        ''')
        logger.info("✅ Table yaratildi: repository_topics")
    def get_top_languages_by_year_and_size(self, year: int, top_n: int = 5) -> List[Dict]:
        """
        Berilgan yil bo'yicha eng ko'p kod hajmiga ega bo'lgan dasturlash tillarini
        ClickHouse yordamida oladi (Django ORM dagi TopRepoLangBy5Year mantiqiga o'xshash).
        """
        query = f'''
            SELECT 
                language,
                sum(size) AS total_size
            FROM github_analytics.repository_languages
            WHERE created_year = {year} AND language != ''
            GROUP BY language
            ORDER BY total_size DESC
            LIMIT {top_n}
        '''
        
        results = self.client.execute(query)
        
        return [
            {
                'language': lang,
                'total_size': size, 
                'year': year
            }
            for lang, size in results
        ]
    
    def insert_repository_date(self, repositories: List[Dict]):
        """Repository ma'lumotlarini ClickHouse ga qo'shish"""
        if not repositories:
            logger.warning("Bo'sh ma'lumotlar ro'yxati")
            return
        
        main_data = []
        language_data = []
        topic_data = []
        
        error_count = 0
        
        for idx, repo in enumerate(repositories):
            try:
                # Sanalarni parse qilish
                created_at = datetime.fromisoformat(repo['createdAt'].replace('Z', '+00:00'))
                pushed_at = datetime.fromisoformat(repo['pushedAt'].replace('Z', '+00:00'))
                created_year = created_at.year

                def clean_int(value, default=0):
                    # None, bo'sh satr, yoki noto'g'ri tur bo'lsa, default qiymatni qaytaradi
                    if value is None or value == '' or not isinstance(value, (int, float)):
                        return default
                    try:
                        # Kiritilgan qiymatni to'liq butun songa aylantirish
                        return int(value)
                    except ValueError:
                        return default
                
                # Asosiy repository ma'lumoti
                main_data.append((
                    repo['owner'],
                    repo['name'],
                    repo['nameWithOwner'],
                    repo.get('description', '') or '',
                    repo['stars'],
                    repo['forks'],
                    repo['watchers'],
                    1 if repo['isFork'] else 0,
                    1 if repo['isArchived'] else 0,
                    repo['languageCount'],
                    repo['topicCount'],
                    repo['diskUsageKb'],
                    repo['pullRequests'],
                    repo['issues'],
                    repo.get('primaryLanguage', '') or '',
                    created_at,
                    pushed_at,
                    created_year,
                    created_at.date(),
                    clean_int(repo.get('defaultBranchCommitCount',0)),
                    repo.get('license', '') or '',
                    repo['assignableUserCount'],
                    repo.get('codeOfConduct', '') or '',
                    1 if repo['forkingAllowed'] else 0,
                    1 if repo.get('parent') else 0
                ))
                
                # Languages ma'lumotlari
                for lang in repo.get('languages', []):
                    language_data.append((
                        repo['nameWithOwner'],
                        lang['name'],
                        lang['size'],
                        created_year,
                        repo['stars'],
                        repo['forks']
                    ))
                
                # Topics ma'lumotlari
                for topic in repo.get('topics', []):
                    topic_data.append((
                        repo['nameWithOwner'],
                        topic['name'],
                        topic.get('stars', 0),
                        created_year,
                        repo['stars']
                    ))
                
                # Har 10000 ta repositorydan keyin progress
                if (idx + 1) % 10000 == 0:
                    logger.info(f"Progress: {idx + 1}/{len(repositories)} ta repository qayta ishlandi")
                
            except Exception as e:
                error_count += 1
                logger.error(f"Repository qayta ishlashda xatolik ({repo.get('nameWithOwner', 'unknown')}): {e}")
                if error_count > 100:
                    logger.error("Ko'p xatoliklar! To'xtatilmoqda...")
                    break
        
        # Asosiy ma'lumotlarni qo'shish
        if main_data:
            logger.info(f"Repositories tableiga {len(main_data)} ta yozuv qo'shilmoqda...")
            try:
                self.client.execute(
                    '''
                    INSERT INTO github_analytics.repositories 
                    (owner, name, name_with_owner, description, stars, forks, watchers, 
                     is_fork, is_archived, language_count, topic_count, disk_usage_kb, 
                     pull_requests, issues, primary_language, created_at, pushed_at, 
                     created_year, created_date, default_branch_commit_count, license, 
                     assignable_user_count, code_of_conduct, forking_allowed, has_parent)
                    VALUES
                    ''',
                    main_data
                )
                logger.info(f"✅ {len(main_data)} ta repository qo'shildi")
            except Exception as e:
                logger.error(f"Repositories tableiga qo'shishda xatolik: {e}")
                raise
        
        # Languages ma'lumotlarini qo'shish
        if language_data:
            logger.info(f"Languages tableiga {len(language_data)} ta yozuv qo'shilmoqda...")
            try:
                self.client.execute(
                    '''
                    INSERT INTO github_analytics.repository_languages 
                    (repo_name_with_owner, language, size, created_year, repo_stars, repo_forks)
                    VALUES
                    ''',
                    language_data
                )
                logger.info(f"✅ {len(language_data)} ta language yozuvi qo'shildi")
            except Exception as e:
                logger.error(f"Languages tableiga qo'shishda xatolik: {e}")
                # Language xatoligi kritik emas, davom ettiramiz
        
        # Topics ma'lumotlarini qo'shish
        if topic_data:
            logger.info(f"Topics tableiga {len(topic_data)} ta yozuv qo'shilmoqda...")
            try:
                self.client.execute(
                    '''
                    INSERT INTO github_analytics.repository_topics 
                    (repo_name_with_owner, topic, topic_stars, created_year, repo_stars)
                    VALUES
                    ''',
                    topic_data
                )
                logger.info(f"✅ {len(topic_data)} ta topic yozuvi qo'shildi")
            except Exception as e:
                logger.error(f"Topics tableiga qo'shishda xatolik: {e}")
                # Topic xatoligi kritik emas, davom ettiramiz
        
        logger.info(f"✅ Import yakunlandi! Xatoliklar: {error_count}")
    
    def get_top_languages_by_year(self, top_n: int = 5) -> Dict[int, List[Dict]]:
        """Yil bo'yicha eng ko'p ishlatiladigan dasturlash tillari"""
        query = '''
            SELECT 
                created_year,
                language,
                count(DISTINCT repo_name_with_owner) as repo_count,
                sum(repo_stars) as total_stars,
                sum(size) as total_code_size
            FROM github_analytics.repository_languages
            WHERE language != '' AND created_year > 0
            GROUP BY created_year, language
            ORDER BY created_year DESC, repo_count DESC
        '''
        
        results = self.client.execute(query)
        
        stats_by_year = {}
        for year, language, repo_count, total_stars, total_code_size in results:
            if year not in stats_by_year:
                stats_by_year[year] = []
            
            if len(stats_by_year[year]) < top_n:
                stats_by_year[year].append({
                    'language': language,
                    'repository_count': repo_count,
                    'total_stars': total_stars,
                    'total_code_size_bytes': total_code_size
                })
        
        return stats_by_year
    
    def get_language_statistics(self) -> List[Dict]:
        """Umumiy dasturlash tillari statistikasi"""
        query = '''
            SELECT 
                language,
                count(DISTINCT repo_name_with_owner) as repo_count,
                sum(size) as total_size,
                avg(repo_stars) as avg_stars,
                max(repo_stars) as max_stars
            FROM github_analytics.repository_languages
            WHERE language != ''
            GROUP BY language
            ORDER BY repo_count DESC
            LIMIT 20
        '''
        
        results = self.client.execute(query)
        return [
            {
                'language': lang,
                'repository_count': count,
                'total_code_size_bytes': total_size,
                'average_stars': round(avg_stars, 2),
                'max_stars': max_stars
            }
            for lang, count, total_size, avg_stars, max_stars in results
        ]
    
    def get_repository_count(self) -> int:
        """Jami repositorylar soni"""
        result = self.client.execute('SELECT count() FROM github_analytics.repositories')
        return result[0][0]
    
    def clear_data(self):
        """Barcha ma'lumotlarni o'chirish"""
        self.client.execute('TRUNCATE TABLE IF EXISTS github_analytics.repositories')
        self.client.execute('TRUNCATE TABLE IF EXISTS github_analytics.repository_languages')
        self.client.execute('TRUNCATE TABLE IF EXISTS github_analytics.repository_topics')
        logger.info("✅ Barcha ma'lumotlar tozalandi")