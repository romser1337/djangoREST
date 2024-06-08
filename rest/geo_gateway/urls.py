from django.urls import path
from . import views
from django.conf.urls.static import static
from django.conf import settings

urlpatterns = [
    path('geodata', views.getData),
    path('get-cells', views.getCells),
] + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)