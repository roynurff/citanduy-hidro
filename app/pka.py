import datetime
from flask import Blueprint, render_template, request, abort
from flask_login import current_user
from peewee import DoesNotExist

from app import get_sampling
from app.models import Pos, HasilUjiKualitasAir
bp = Blueprint('pka', __name__, url_prefix='/pka')


@bp.route('/map')
def map():
    poska = Pos.select().where(Pos.tipe=='4').order_by(Pos.sungai)
    ctx = {
        'poses': poska
    }
    return render_template('pka/map.html', ctx=ctx)

@bp.route('/')
def index():
    tahun = request.args.get('tahun', datetime.date.today().year, type=int)
    
    # Get all data untuk tahun ini
    all_data = (HasilUjiKualitasAir.select()
                .where(HasilUjiKualitasAir.sampling.year == tahun)
                .order_by(HasilUjiKualitasAir.sampling))
    
    # Organize data by bulan (month)
    bulan_data = {}
    bulan_names = {
        1: 'Januari', 2: 'Februari', 3: 'Maret', 4: 'April', 5: 'Mei', 6: 'Juni',
        7: 'Juli', 8: 'Agustus', 9: 'September', 10: 'Oktober', 11: 'November', 12: 'Desember'
    }
    
    for hu in all_data:
        bulan = hu.sampling.month
        if bulan not in bulan_data:
            bulan_data[bulan] = {
                'bulan': bulan_names[bulan],
                'bulan_num': bulan,
                'periode': hu.periode,
                'data': [],
                'count_total': 0,
                'count_memenuhi': 0,
                'count_cemar_ringan': 0,
                'count_cemar_sedang': 0,
                'count_cemar_berat': 0,
                'sampling_date': hu.sampling
            }
        
        bulan_data[bulan]['data'].append(hu)
        bulan_data[bulan]['count_total'] += 1
        
        # Count status
        if hu.status_hasil_uji == 'memenuhi baku mutu':
            bulan_data[bulan]['count_memenuhi'] += 1
        elif hu.status_hasil_uji == 'cemar ringan':
            bulan_data[bulan]['count_cemar_ringan'] += 1
        elif hu.status_hasil_uji == 'cemar sedang':
            bulan_data[bulan]['count_cemar_sedang'] += 1
        elif hu.status_hasil_uji == 'cemar berat':
            bulan_data[bulan]['count_cemar_berat'] += 1
    
    # Sort by bulan
    bulan_list = sorted(bulan_data.values(), key=lambda x: x['bulan_num'])
    
    ctx = {
        'tahun': tahun,
        'now': datetime.date.today(),
        'bulan_data': bulan_list,
        'bulan_names': bulan_names
    }
    return render_template('pka/index.html', ctx=ctx)

@bp.route('/publikasi/<int:bulan>')
def publikasi(bulan):
    tahun = request.args.get('tahun', datetime.date.today().year, type=int)
    
    # Get data untuk bulan dan tahun tertentu
    data_ka = (HasilUjiKualitasAir.select()
               .where(HasilUjiKualitasAir.sampling.year == tahun,
                      HasilUjiKualitasAir.sampling.month == bulan)
               .order_by(HasilUjiKualitasAir.lokasi, HasilUjiKualitasAir.sampling))
    
    if not data_ka:
        abort(404)
    
    bulan_names = {
        1: 'Januari', 2: 'Februari', 3: 'Maret', 4: 'April', 5: 'Mei', 6: 'Juni',
        7: 'Juli', 8: 'Agustus', 9: 'September', 10: 'Oktober', 11: 'November', 12: 'Desember'
    }
    
    ctx = {
        'tahun': tahun,
        'bulan': bulan,
        'bulan_nama': bulan_names[bulan],
        'now': datetime.date.today(),
        'data': data_ka
    }
    return render_template('pka/publikasi.html', ctx=ctx)


'''
Cukangleuleus, 22 Agustus 2023
No Parameter Satuan Ci Ci/Lij (Ci/Lih)baru
1 Temperatur oC 28,00 
2 DO mg/l 6,00 1,50 0,50
3 pH mg/l 7,70 1,03 0,13
4 DHL umhos -
5 Kekeruhan mg/l 15,00
6 BOD mg/l 2,00 0,67 0,67
7 COD mg/l 14,00 0,56 0,56
8 TSS mg/l 11,00 0,22 0,22
9 Total Nitrogen mg/l -
10 Nitrit mg/l 0,90 15,00 6,88
11 Nitrat mg/l 0,02 0,002 0,002
12 Amoniak mg/l -
13 Total Fosfat mg/l 0,10 0,50 0,50
14 Ortho Fosfat mg/l -
15 Deterjen mg/l 0,08 0,0004 0,0004
16 Minyak Lemak mg/l - - -
17 Phenol mg/l - - -
18 Fecal Coliform mg/l 2.100,00 2,10 2,61
19 Total Coliform mg/l 3.300,00 0,66 0,66
20 Mercury mg/l - - -
21 Cadmium mg/l 0,0001 0,01 0,01
22 Chromium mg/l - - -
23 Timbal mg/l - - -
24 Tembaga mg/l 0,02 0,80 0,80
25 Besi mg/l -
26 Mangan mg/l -
27 Seng mg/l 0,04 0,80 0,80
28 Chlorida mg/l -
29 Boron mg/l - - -
30 Sulfida mg/l - - -
31 Sulfat mg/l -
32 Fluorida mg/l - - -
33 Selenium mg/l - - -
34 Sianida mg/l - - -
35 Arsen mg/l - - -
Cemar Ringan

CUkangleuleus, 19 September 2023

1 Temperatur 0C 30,00
2 DO mg/l 7,90 1,98 0,02
3 pH mg/l 8,40 1,12 0,60
4 DHL umhos -
5 Kekeruhan mg/l 11,00
6 BOD mg/l 3,40 1,13 1,27
7 COD mg/l 10,00 0,40 0,40
8 TSS mg/l 10,00 0,200 0,200
9 Total Nitrogen mg/l -
10 Nitrit mg/l 0,90 15,00 6,88
11 Nitrat mg/l 0,02 0,002 0,002
12 Amoniak mg/l -
13 Total Fosfat mg/l 0,10 0,50 0,50
14 Ortho Fosfat mg/l -
15 Deterjen mg/l 0,08 0,0004 0,0004
16 Minyak Lemak mg/l - - -
17 Phenol mg/l - - -
18 Fecal Coliform mg/l 68,00 0,07 0,07
19 Total Coliform mg/l 1.300,00 5000 0,26 0,26
20 Mercury mg/l - - -
21 Cadmium mg/l 0,0001 0,01 0,01
22 Chromium mg/l - - -
23 Timbal mg/l - - -
24 Tembaga mg/l 0,02 0,80 0,80
25 Besi mg/l -
26 Mangan mg/l -
27 Seng mg/l 0,04 0,80 0,80
28 Chlorida mg/l -
29 Boron mg/l - - -
30 Sulfida mg/l - - -
31 Sulfat mg/l -
32 Fluorida mg/l - - -
33 Selenium mg/l - - -
34 Sianida mg/l - - -
35 Arsen mg/l - - -
Cemar Ringan

CUkangleuleus, 24 Oktober 2023

1 Temperatur 0C 29,00
2 DO mg/l 6,80 1,70 0,30
3 pH mg/l 7,80 1,04 0,20
4 DHL umhos -
5 Kekeruhan mg/l 12,00
6 BOD mg/l 2,80 0,93 0,93
7 COD mg/l 17,00 0,68 0,68
8 TSS mg/l 6,00 0,120 0,120
9 Total Nitrogen mg/l -
10 Nitrit mg/l 0,90 15,00 6,88
11 Nitrat mg/l 0,02 0,002 0,002
12 Amoniak mg/l -
13 Total Fosfat mg/l 0,10 0,50 0,50
14 Ortho Fosfat mg/l -
15 Deterjen mg/l 0,08 0,0004 0,0004
16 Minyak Lemak mg/l - - -
17 Phenol mg/l - - -
18 Fecal Coliform mg/l 920,00 0,92 0,92
19 Total Coliform mg/l 5.400,00 1,08 1,17
20 Mercury mg/l - - -
21 Cadmium mg/l 0,0001 0,01 0,01
22 Chromium mg/l - - -
23 Timbal mg/l - - -
24 Tembaga mg/l 0,02 0,80 0,80
25 Besi mg/l -
26 Mangan mg/l -
27 Seng mg/l 0,04 0,80 0,80
28 Chlorida mg/l -
29 Boron mg/l - - -
30 Sulfida mg/l - - -
31 Sulfat mg/l -
32 Fluorida mg/l - - -
33 Selenium mg/l - - -
34 Sianida mg/l - - -
35 Arsen mg/l -
Cemar Ringan


Rajapolah, 15 Juli 2024
No, Parameter, Satuan, Hasil Uji, Kadar Maksimum, Deviasi, Metode Pengujian
1 Temperatur °C
2 Padatan Terlarut Total (TDS) mg/L
3 Padatan Tersuspensi Total (TSS) mg/L SM APHA 24th Ed, 2540 D, 2023
4 Derajat keasaman (pH) -
5 Kebutuhan Oksigen Biokimiawi (BOD) mg/L
6 Kebutuhan Oksigen Kimiawi (COD) mg/L
7 Oksigen terlarut (DO) mg/L
8 Nitrat (sebagai N) mg/L IK-22-PVM-TP Spektrofotometri
9 Nitrit (sebagai N) mg/L
10 Total Fosfat (sebagai P) mg/L
11 Kadmium (Cd) Terlarut mg/L
12 Tembaga (Cu) terlarut mg/L
13 Seng (Zn)Terlarut mg/L
14 Deterjen Total mg/L
15 Fecal Coliform MPN/100mL
16 Total Coliform MPN/100mL
17 Kekeruhan NTU SNI 06-6989.25-2005
Catatan : Hasil pengujian ini hanya berlaku terhadap contoh uji yang diambil oleh UPTD Laboratorium Lingkungan Hidup
Catatan : Kondisi lingkungan pada saat pengambilan contoh uji : (1) Suhu : 23,5 oC; (2) kelembaban : 63,1% (3)Cuaca : Berawan
Catatan : Tanda lebih kecil (<) menunjukkan hasil uji lebih kecil dari LoQ. Nilai LoQ adalah jumlah analit terkecil dalam contoh uji yang dapat
Catatan : diukur oleh laboratorium dengan akurat dan presisi yang diyakini

'''