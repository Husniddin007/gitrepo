from django.db.models import Sum
from django.db.models.functions import ExtractYear
from rest_framework.views import APIView
from rest_framework.response import Response
from app.models import RepositoryLanguage

class TopLanguagesByYear(APIView):
    def get(self, request):
        data = (
            RepositoryLanguage.objects
            .values(year=ExtractYear('repository__created_at'), language='language__name')
            .annotate(total_size=Sum('size'))
            .order_by('year', '-total_size')
        )

        result = {}
        for item in data:
            year = str(item['year'])
            result.setdefault(year, [])
            if len(result[year]) < 5:
                result[year].append({
                    'language': item['language'],
                    'total_size': item['total_size']
                })

        return Response(result)

    

