# Generated by Django 5.1.6 on 2025-03-23 20:11

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Bhavcopy',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('SYMBOL', models.CharField(max_length=20)),
                ('SERIES', models.CharField(max_length=5)),
                ('DATE1', models.DateField()),
                ('PREV_CLOSE', models.FloatField()),
                ('OPEN_PRICE', models.FloatField()),
                ('HIGH_PRICE', models.FloatField()),
                ('LOW_PRICE', models.FloatField()),
                ('LAST_PRICE', models.FloatField()),
                ('CLOSE_PRICE', models.FloatField()),
                ('AVG_PRICE', models.FloatField()),
                ('TTL_TRD_QNTY', models.IntegerField()),
                ('TURNOVER_LACS', models.FloatField()),
                ('NO_OF_TRADES', models.IntegerField()),
                ('DELIV_QTY', models.IntegerField()),
                ('DELIV_PER', models.FloatField()),
            ],
            options={
                'verbose_name': 'Bhavcopy Data',
                'verbose_name_plural': 'Bhavcopy Data',
                'unique_together': {('SYMBOL', 'SERIES', 'DATE1')},
            },
        ),
    ]
