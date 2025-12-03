from rest_framework import serializers

class TopRepoSerializer(serializers.Serializer):
    language = serializers.CharField()
    total_size = serializers.IntegerField()
    year = serializers.IntegerField()
    