from django.urls import path
from app.views import TopLanguagesByYear


urlpatterns = [
    path('top5-languages',TopLanguagesByYear.as_view(),name='top5-languages'),
]