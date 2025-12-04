from django.urls import path
# E'tibor bering, barcha viewlar .views dan import qilinishi kerak
from .views import TopRepoLangBy5Year, RepositoryStatisticsView, TopRepoLangByYearCH
# Agar ClickHouse uchun alohida View yaratgan bo'lsak, uni ham qo'shamiz

urlpatterns = [
    # 1. ClickHouse dan umumiy statistika
    path('statistics', RepositoryStatisticsView.as_view(), name='statistics'),
    
    # 2. ORM/Kesh orqali top 5 til
    path('top5-languages', TopRepoLangBy5Year.as_view(), name='top5-languages'),  
    
    # 3. Agar ClickHouse uchun yangi View ni ishlatmoqchi bo'lsangiz:
    path('ch-top-languages', TopRepoLangByYearCH.as_view(), name='ch-top-languages'),
]