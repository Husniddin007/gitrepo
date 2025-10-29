import json
from django.core.management.base import BaseCommand
from app.models import Repository, Language, RepositoryLanguage, Topic
from datetime import datetime

class Command(BaseCommand):
    help = 'hello'

    def add_arguments(self, parser):
        parser.add_argument('jsonfile', type=str)

    def handle(self, *args, **kwargs):
        file_path = kwargs['jsonfile']
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        for repo_data in data:
            repo, created = Repository.objects.get_or_create(
                owner=repo_data['owner'],
                name=repo_data['name'],
                defaults={
                    'name_with_owner': repo_data.get('nameWithOwner'),
                    'description': repo_data.get('description'),
                    'stars': repo_data.get('stars', 0),
                    'forks': repo_data.get('forks', 0),
                    'watchers': repo_data.get('watchers', 0),
                    'issues': repo_data.get('issues', 0),
                    'pull_requests': repo_data.get('pullRequests', 0),
                    'disk_usage_kb': repo_data.get('diskUsageKb', 0),
                    'assignable_user_count': repo_data.get('assignableUserCount', 0),
                    'default_branch_commit_count': repo_data.get('defaultBranchCommitCount', 0),
                    'is_fork': repo_data.get('isFork', False),
                    'is_archived': repo_data.get('isArchived', False),
                    'forking_allowed': repo_data.get('forkingAllowed', True),
                    'code_of_conduct': repo_data.get('codeOfConduct'),
                    'license': repo_data.get('license'),
                    'created_at': datetime.fromisoformat(repo_data['createdAt'].replace('Z', '+00:00')),
                    'pushed_at': datetime.fromisoformat(repo_data['pushedAt'].replace('Z', '+00:00')),
                }
            )
            if repo_data.get('primaryLanguage'):
                lang_obj, _ = Language.objects.get_or_create(name=repo_data['primaryLanguage'])
                repo.primary_language = lang_obj
                repo.save()


            for lang in repo_data.get('languages', []):
                lang_obj, _ = Language.objects.get_or_create(name=lang['name'])
                RepositoryLanguage.objects.create(repository=repo, language=lang_obj, size=lang['size'])

            for topic in repo_data.get('topics', []):
                topic_obj, _ = Topic.objects.get_or_create(name=topic['name'], defaults={'stars': topic.get('stars', 0)})
                repo.topics.add(topic_obj)
        
        print('AAAAAAAAA')

        self.stdout.write(self.style.SUCCESS('Success!'))
