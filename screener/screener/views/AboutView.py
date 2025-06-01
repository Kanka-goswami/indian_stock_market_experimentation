from django.http import HttpResponse

def about(request):
    return HttpResponse("This is a project for analyzing the stock market.")