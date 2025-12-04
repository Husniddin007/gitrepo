# management/commands/ingest_to_clickhouse.py

from django.core.management.base import BaseCommand
import json
from app.services.clickhouse_service import ClickHouseService

class Command(BaseCommand):
    help = 'GitHub repository ma\'lumotlarini ClickHouse ga import qilish'

    def add_arguments(self, parser):
        parser.add_argument('json_file', type=str, help='JSON fayl yo\'li')
        parser.add_argument(
            '--batch-size',
            type=int,
            default=5000,
            help='Bir vaqtning o\'zida yuklanadigan repositorylar soni (default: 5000)'
        )

    def handle(self, *args, **options):
        json_file = options['json_file']
        batch_size = options['batch_size']
        
        self.stdout.write(f'📂 Fayl o\'qilyapti: {json_file}')
        
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            total = len(data)
            self.stdout.write(f'📊 {total} ta repository topildi')
            self.stdout.write(f'⚙️  Batch size: {batch_size}')
            
            # ClickHouse serviceini yaratish
            ch_service = ClickHouseService()

            self.stdout.write('⏳ ClickHouse Database va jadvallari yaratilmoqda/tekshirilmoqda...')
            ch_service.create_database_and_table()
            self.stdout.write(self.style.SUCCESS('✅ Database va jadvallar tayyor'))
            
            # Batch qilib yuklash
            for i in range(0, total, batch_size):
                batch = data[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                total_batches = (total + batch_size - 1) // batch_size
                
                self.stdout.write(f'\n⏳ Batch {batch_num}/{total_batches} yuklanmoqda ({len(batch)} ta repository)...')
                
                try:
                    ch_service.insert_repository_date(batch)
                    self.stdout.write(self.style.SUCCESS(f'✅ Batch {batch_num} muvaffaqiyatli yuklandi'))
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f'❌ Batch {batch_num} da xatolik: {e}'))
                    # Xatolik bo'lsa ham davom etish
            
            # Yakuniy statistika
            total_in_db = ch_service.get_repository_count()
            self.stdout.write(self.style.SUCCESS(f'\n🎉 Import yakunlandi!'))
            self.stdout.write(self.style.SUCCESS(f'   - Faylda: {total}'))
            self.stdout.write(self.style.SUCCESS(f'   - Bazada: {total_in_db}'))
            
        except FileNotFoundError:
            self.stdout.write(self.style.ERROR(f'❌ Fayl topilmadi: {json_file}'))
        except json.JSONDecodeError as e:
            self.stdout.write(self.style.ERROR(f'❌ JSON parse xatoligi: {e}'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Umumiy xatolik: {e}'))
            import traceback
            traceback.print_exc()