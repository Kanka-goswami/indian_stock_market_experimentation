from rest_framework import serializers
from bhavcopy.models import Bhavcopy

class BhavcopySerializer(serializers.ModelSerializer):
    class Meta:
        model = Bhavcopy
        fields = '__all__'