from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.db.models import Sum
from django.core.cache import cache
from .models import RepoLanguage

from .serializers import TopRepoSerializer
import logging


# Agar ORM ishlatilayotgan bo'lsa:
# from .models import RepoLanguage
# from .serializers import TopRepoSerializer

# ClickHouse service
from app.services.clickhouse_service import ClickHouseService

logger = logging.getLogger(__name__)

# --- 1. Django ORM + Kesh orqali Top 5 tillar (Avvalgi kodingiz) ---
# class TopRepoLangBy5Year(APIView):
#     def get(self, request):
#         # Bu qism ORM (PostgreSQL/MySQL) ishlatilayotgan bo'lsa
#         # va sizning .models da RepoLanguage, Serializer kiritilgan bo'lsa ishlaydi.
#         # Hozircha buni ClickHouse ga o'zgartirib foydalanishni tavsiya etamiz.
#         return Response({"detail": "This endpoint uses Django ORM. Use 'ch-top-languages' for ClickHouse."}, 
#                         status=status.HTTP_501_NOT_IMPLEMENTED)

class TopRepoLangBy5Year(APIView):

    def get(self, request):
        try:
            year = int(request.query_params.get('year',2025))
        except ValueError:
            return Response({"detail": "year must be integer"}, status=status.HTTP_400_BAD_REQUEST)
        try:
            limit = int(request.query_params.get('limit', 5))
        except ValueError:
            limit = 5
        cache_key = f"top_langs_by_size: {year}:{limit}"
        cached = cache.get(cache_key)
        if cached is not None:
            return Response(cached)
        qs = (
            RepoLanguage.objects
            .filter(repo__created_year=year)
            .values('language__name')
            .annotate(total_size=Sum('size'))
            .order_by('-total_size')[:limit]
            )
        data = [
        {
            'language': item.get('language__name'),
            'total_size': int(item.get('total_size') or 0),
            'year': year,
        }
            for item in qs
    ]
        cache.set(cache_key, data, 60*60)
        serializer = TopRepoSerializer(data,many=True)
        return Response(serializer.data)


# --- 2. ClickHouse orqali umumiy statistika (Siz kiritgan qism) ---
class RepositoryStatisticsView(APIView):
    """ClickHouse dan umumiy til statistikasini oladi."""
    def get(self, request):
        try:
            service = ClickHouseService()
            # get_repository_statistics o'rniga get_language_statistics ishlatish tavsiya etiladi
            stats = service.get_language_statistics() 
            
            if not stats:
                return Response([], status=status.HTTP_204_NO_CONTENT)
                
            return Response(stats, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"ClickHouse umumiy statistika xatosi: {e}")
            return Response(
                {'error': f'ClickHouse serveri yoki so\'rovda xatolik: {str(e)}'}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

# --- 3. ClickHouse + Kesh orqali yillik Top tillar (Yangilangan) ---
class TopRepoLangByYearCH(APIView):
    """ClickHouse dan berilgan yil bo'yicha eng yaxshi tillarni oladi (Kesh bilan)."""
    def get(self, request):
        try:
            year = int(request.query_params.get('year', 2025))
        except ValueError:
            return Response({"detail": "year butun son bo'lishi kerak."}, status=status.HTTP_400_BAD_REQUEST)

        try:
            limit = int(request.query_params.get('limit', 5))
        except ValueError:
            limit = 5
            
        
        cache_key = f"ch_top_langs_by_year:{year}:{limit}"
        cached_data = cache.get(cache_key)

        if cached_data is not None:
            return Response(cached_data)

        try:
            service = ClickHouseService()
            # ClickHouse service ichidagi to'g'ri metodni chaqiramiz
            stats = service.get_top_languages_by_year_and_size(year=year, top_n=limit) 
            
        except Exception as e:
            logger.error(f"ClickHouse so'rovida xatolik ({cache_key}): {e}")
            return Response(
                {'error': f'ClickHouse so\'rovida xatolik: {str(e)}'}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        
        cache.set(cache_key, stats, 60 * 3) # 1 soat keshga saqlash

        return Response(stats, status=status.HTTP_200_OK)