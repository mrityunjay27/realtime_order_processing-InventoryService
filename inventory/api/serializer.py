from rest_framework import serializers
from ..models import Product, Inventory


class ProductSerializer(serializers.ModelSerializer):
    class Meta:
        model = Product
        fields = ["id", "name", "description", "price", "created_at"]
        read_only_fields = ["id", "created_at"]


class InventorySerializer(serializers.ModelSerializer):
    class Meta:
        model = Inventory
        fields = ["product", "available_quantity", "reserved_quantity", "updated_at"]
        read_only_fields = ["updated_at"]
