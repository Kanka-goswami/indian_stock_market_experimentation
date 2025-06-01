from django.http import HttpResponse

def homepage(request):
    return HttpResponse("Stock Screener")