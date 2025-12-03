from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.db.models import Sum
from django.core.cache import cache
from .models import RepoLanguage
from .serializers import TopRepoSerializer

class TopRepoLangBy5Year(APIView):
    def get(self, request):
        print(f"request ===== {request}")
        try:
            year = int(request.query_params.get('year',2024))
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
    

