from django.urls import path
from . import views
from django.conf.urls.static import static
from django.conf import settings

urlpatterns = [
    path('geodata', views.get_coverage),
    path('get-cells', views.get_cells),
    path('dismantle-site', views.dismantle_site),
] + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)