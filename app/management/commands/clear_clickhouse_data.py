# app/management/commands/clear_clickhouse_data.py

from django.core.management.base import BaseCommand
from app.services.clickhouse_service import ClickHouseService
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = "ClickHouse dagi barcha 'github_analytics' ma'lumotlarini o'chiradi (TRUNCATE)."

    def handle(self, *args, **options):
        self.stdout.write("⏳ ClickHouse ma'lumotlarini tozalash jarayoni boshlanmoqda...")
        
        try:
            ch_service = ClickHouseService()
            
            # clear_data funksiyasini chaqirish
            ch_service.clear_data()
            
            self.stdout.write(self.style.SUCCESS("🎉 Barcha 'github_analytics' ma'lumotlari muvaffaqiyatli tozalandi."))
        
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ Ma'lumotlarni tozalashda xatolik yuz berdi: {e}"))
            logger.error(f"ClickHouse ma'lumotlarini tozalashda xato: {e}")
            raise