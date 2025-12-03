from django.urls import path
from app.views import TopRepoLangBy5Year


urlpatterns = [
    path('top5-languages',TopRepoLangBy5Year.as_view(),name='top5-languages'),
]