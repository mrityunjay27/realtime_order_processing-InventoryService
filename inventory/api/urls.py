from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import ProductViewSet, InventoryCreateView, InventoryUpdateView

router = DefaultRouter()
router.register("products", ProductViewSet)

urlpatterns = [
    path("", include(router.urls)),
    path("inventory/", InventoryCreateView.as_view(), name="inventory-create"),
    path("inventory/<uuid:product_id>/", InventoryUpdateView.as_view(), name="inventory-update"),
]
