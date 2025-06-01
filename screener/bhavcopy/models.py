from django.db import models

# Create your models here.
from django.db import models

class Bhavcopy(models.Model):
    SYMBOL = models.CharField(max_length=20)
    SERIES = models.CharField(max_length=5)
    DATE1 = models.DateField()
    PREV_CLOSE = models.FloatField()
    OPEN_PRICE = models.FloatField()
    HIGH_PRICE = models.FloatField()
    LOW_PRICE = models.FloatField()
    LAST_PRICE = models.FloatField()
    CLOSE_PRICE = models.FloatField()
    AVG_PRICE = models.FloatField()
    TTL_TRD_QNTY = models.IntegerField()
    TURNOVER_LACS = models.FloatField()
    NO_OF_TRADES = models.IntegerField()
    DELIV_QTY = models.IntegerField()
    DELIV_PER = models.FloatField()
    
    def __str__(self):
        return f"{self.SYMBOL} - {self.DATE1}"
    
    class Meta:
        unique_together = ('SYMBOL', 'SERIES', 'DATE1')
        verbose_name = 'Bhavcopy Data'
        verbose_name_plural = 'Bhavcopy Data'