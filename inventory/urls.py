from django.urls import path, include

urlpatterns = [
    path("", include("inventory.api.urls")),
]
