from rest_framework import serializers
from app.models import Employee,Language



class EmployeeSerializer(serializers.ModelSerializer):
    class Meta:
        model = Employee
        field = [
            'owner',
            'name',
            'languages',
            'createdAt',
        ]

class LanguageSerializer(serializers.ModelSerializer):
    class Meta:
        model = Language
        field = [
            'name',
        ]
