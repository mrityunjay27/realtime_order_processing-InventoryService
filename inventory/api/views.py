from rest_framework import viewsets, generics
from ..models import Product, Inventory
from .serializer import ProductSerializer, InventorySerializer


class ProductViewSet(viewsets.ModelViewSet):
    queryset = Product.objects.all()
    serializer_class = ProductSerializer


class InventoryCreateView(generics.CreateAPIView):
    queryset = Inventory.objects.all()
    serializer_class = InventorySerializer


class InventoryUpdateView(generics.UpdateAPIView):
    queryset = Inventory.objects.all()
    serializer_class = InventorySerializer
    lookup_field = "product_id"
    lookup_url_kwarg = "product_id"
    http_method_names = ["patch"]
