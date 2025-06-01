"""
URL configuration for screener project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from screener.views.HomeView import homepage
from screener.views.AboutView import about
from bhavcopy.views import FetchBhavcopyDataView
from bhavcopy.yearly_bhavcopy_download_views import YearlyBhavcopyDownloaderView


urlpatterns = [
    path('admin/', admin.site.urls),
    path('', homepage),
    path('about/', about),
    path('fetch-bhavcopy/', FetchBhavcopyDataView.as_view(), name='fetch-bhavcopy'),
    path('YearlyBhavcopyDownloaderView/', YearlyBhavcopyDownloaderView.as_view(), name='YearlyBhavcopyDownloaderView')
    
]
