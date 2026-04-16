import requests
from app.models import RDaily, Pos, ManualDaily, FetchLog, LuwesPos, OPos
from app.config import SOURCE_A, SOURCE_B, SOURCE_C, BOT_TOKEN, CTY_OFFICE_ID, SOURCE_C2
import click
import datetime
import json


def register(app):
    @app.cli.command('send-terlambat-pda7')
    def send_terlambat_pda():
        today = datetime.date.today()
        pdas = Pos.select().where(Pos.tipe=='2')
        mds = ManualDaily.select().where(ManualDaily.sampling==today)
        #print(','.join([p.nama for p in pchs]))
        #print('PEMISAH')
        #print(','.join([m.pos.nama for m in mds if m.pos]))
        msg = 'Data Manual PDA Belum Diterima\n\n'
        msg += '*Tanggal: ' + today.strftime('%d %b %Y*\n')
        late = [p for p in pdas if p.nama not in [m.pos.nama for m in mds if m.pos]]
        msg += 'Jam: ' + datetime.datetime.now().strftime('%H:%M\n')
        msg += '{:.1f}'.format((len(late) / pdas.count()) * 100) + '% (' + str(len(late)) + '/'+ str(pdas.count())+') data belum diterima.\n\n'
        msg += '\n'.join(['{}: {}'.format(p.nama, ','.join([pt.nama for pt in p.petugas_set]) or '-') for p in late])
        if BOT_TOKEN and CTY_OFFICE_ID and msg:
            url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage?chat_id={CTY_OFFICE_ID}&text={msg}'
            resp = requests.get(url)
        else:
            print("BOT_TOKEN, CTY_OFFICE_ID, or msg is None. Cannot send message.")

    @app.cli.command('send-terlambat-pch')
    def send_terlambat_pch():
        today = datetime.date.today()
        pchs = Pos.select().where(Pos.tipe=='1')
        mds = ManualDaily.select().where(ManualDaily.sampling==today - datetime.timedelta(days=1))
        #print(','.join([p.nama for p in pchs]))
        #print('PEMISAH')
        #print(','.join([m.pos.nama for m in mds if m.pos]))
        msg = 'Data Manual PCH Belum Diterima\n\n'
        msg += '*Tanggal: ' + today.strftime('%d %b %Y*\n')
        late = [p for p in pchs if p.nama not in [m.pos.nama for m in mds if m.pos]]
        msg += 'Jam: ' + datetime.datetime.now().strftime('%H:%M\n')
        msg += '{:.1f}'.format((len(late) / pchs.count()) * 100) + '% (' + str(len(late)) + '/'+ str(pchs.count())+') data belum diterima.\n\n'
        msg += '\n'.join(['{}: {}'.format(p.nama, ','.join([pt.nama for pt in p.petugas_set]) or '-') for p in late])
        url = 'https://api.telegram.org/bot' + BOT_TOKEN + '/sendMessage?chat_id=' + CTY_OFFICE_ID + '&text=' + msg
        resp = requests.get(url)
        
    @app.cli.command('fetch-sda-aws')
    def fetch_sdatelemetry_aws():
        '''Membaca data pada server SDATELEMETRY AWS'''
        lokasi_id = ['awskertamukti', 'AWSR01']
        # hardcoded pos_id
        pos_id_map = {'awskertamukti': 93, 'AWSR01': 70}
        sampling = datetime.datetime.now()
        start = sampling - datetime.timedelta(days=1)
        end = sampling
        for nama in lokasi_id:
            base_url = "https://sdatelemetry.com/API_ap_telemetry/loc_datatelemetry_awsnew.php?"
            params = f"nama_lokasi={nama}&dt={start}&dtf={end}"
            url = base_url + params
            x = requests.get(url)
            body = x.text
            start_index = body.find('{"data_telemetryjakarta":')
            json_string = body[start_index:]
            try:
                data = json.loads(json_string)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON from response: {e}")
                print(f"Response body: {body}")
                continue
            field_source = "Rain|Bar|WSpeed|WDir|ATemp|AHum|Rad|Batt|Sinyal"
            field_dest = "rain|barometer|wind_speed|wind_dir|temperature|humidity|radiation|battery|signal"
            lines = data.get('data_telemetryjakarta', [])
            new_raw = []
            for line in lines:
                sampling = line.get('ReceivedDate', '') + 'T' + line.get('ReceivedTime', '')
                row = {"sampling": sampling}
                for field in field_source.split('|'):
                    if field in line:
                        try:
                            value = float(line[field])
                        except ValueError:
                            value = line[field]
                        field = field_dest.split('|')[field_source.split('|').index(field)]
                        row.update({field: value})
                #print(row)
                new_raw.append(row)
                
            # Update latest_sampling di OPos
            try:
                op = OPos.get(OPos.nama==nama)
                op.latest_sampling = datetime.datetime.fromisoformat(new_raw[-1].get('sampling'))
                op.save()
            except OPos.DoesNotExist:
                tipe = '3'
                op = OPos.create(nama=nama, source='SA', tipe=tipe, latest_sampling=datetime.datetime.fromisoformat(new_raw[-1].get('sampling')))
                
            rd, created = RDaily.get_or_create(source='SA',
                                               nama=nama, 
                                               sampling=end.date(), 
                                               defaults={'raw': json.dumps(new_raw),
                                                         'pos_id': pos_id_map.get(nama)})
            if not created:
                rd.raw = json.dumps(new_raw)
                rd.save()


    @app.cli.command('fetch-sda')
    def fetch_sdatelemetry():
        '''Membaca data pada server SDATELEMETRY'''
        # Fallback to working endpoint if SOURCE_A not configured
        url = SOURCE_A or 'https://sdatelemetry.com/API_ap_telemetry/datatelemetry.php?idbbws=8'
        x = requests.get(url)
        fl = FetchLog.create(url=x.url, response=x.status_code, body=x.text, source='SA')
        fl.sa_to_daily()
            
    @app.cli.command('fetch-telemet')
    def fetch_telemet():
        '''Membaca data pada server Omtronik'''
        x = requests.get(SOURCE_B)
        body = ''
        inside = False
        for l in x.text.split('\n'):
            if l.startswith('<table'):
                inside = True
            if l.startswith('</table'):
                body += l
                inside = False
            if len(l) > 3 and inside:
                if l.startswith('<td>Date') or l.startswith('<td>RTU') or l.startswith('<td>Chann') or l.startswith('<td>Value') or l.startswith('<td>Satuan'):
                    pass
                else:
                    body += l
                    
        fl = FetchLog.create(url=x.url, response=x.status_code, body=body, source='SB')
        fl.sb_to_daily()

    @app.cli.command('fetch-luwes')
    def fetch_luwes():
        '''Membaca data dari luwes'''
        for l in LuwesPos.select():
            data = {'a': 'stat', 'imei': l.imei}
            try:
                x = requests.post(SOURCE_C2, data=data)
                if x.status_code == 200 and 'error' not in x.text.lower():
                    fl = FetchLog.create(url=x.url, response=x.status_code, body=x.text, source='SC')
                    fl.sc_to_daily()
            except Exception as e:
                print("Error fetching from SOURCE_C2", e)
                print(data)
                pass
            
    
    
######################## DEVELOPMENT ONLY #########################
    @app.cli.command('ksi_to_daily')
    def ksi_to_daily():
        '''
        all_rec = 
            RW67_Manganti_2024-10-17
            2024-10-17 00:00:00 {'battery': 12.7, 'rain': 0.0, 'wlevel': 10.2}
            2024-10-17 00:15:00 {'rain': 0.0}
            2024-10-17 00:30:00 {'rain': 0.0}
            2024-10-17 00:45:00 {'rain': 0.0}
            2024-10-17 01:00:00 {'battery': 12.7, 'rain': 0.0, 'wlevel': 10.2}
            2024-10-17 01:15:00 {'rain': 0.0}
            2024-10-17 01:30:00 {'rain': 0.0}
            2024-10-17 01:45:00 {'rain': 0.0}
            2024-10-17 02:00:00 {'battery': 12.6, 'rain': 0.0, 'wlevel': 10.2}
            2024-10-17 02:15:00 {'rain': 0.0}
            2024-10-17 02:30:00 {'rain': 0.0}
            2024-10-17 02:45:00 {'rain': 0.0}
            2024-10-17 03:00:00 {'battery': 12.6, 'rain': 0.0, 'wlevel': 10.2}
            2024-10-17 03:15:00 {'rain': 0.0}
            2024-10-17 03:30:00 {'rain': 0.0}
            2024-10-17 03:45:00 {'rain': 0.0}
            2024-10-17 04:00:00 {'battery': 12.6, 'rain': 0.0, 'wlevel': 10.2}
            2024-10-17 04:15:00 {'rain': 0.0}
            2024-10-17 04:30:00 {'rain': 0.0}
            2024-10-17 04:45:00 {'rain': 0.0}
            2024-10-17 05:00:00 {'battery': 12.6, 'rain': 0.0, 'wlevel': 10.2}
            2024-10-17 05:15:00 {'rain': 0.0}
            2024-10-17 05:30:00 {'rain': 0.0}
            2024-10-17 05:45:00 {'rain': 0.0}
            2024-10-17 06:00:00 {'battery': 12.6, 'rain': 0.0, 'wlevel': 10.2}
            2024-10-17 06:15:00 {'rain': 0.0}
            2024-10-17 06:30:00 {'rain': 0.0}
            2024-10-17 06:45:00 {'rain': 0.0}
            2024-10-17 07:00:00 {'battery': 13.3, 'rain': 0.0, 'wlevel': 10.2}
            2024-10-17 07:15:00 {'rain': 0.0}
            2024-10-17 07:30:00 {'rain': 0.0}
            2024-10-17 07:45:00 {'rain': 0.0}
            2024-10-17 08:00:00 {'battery': 14.1, 'rain': 0.0, 'wlevel': 10.2}
            2024-10-17 08:15:00 {'rain': 0.0}
            2024-10-17 08:30:00 {'rain': 0.0}
            2024-10-17 08:45:00 {'rain': 0.0}
            2024-10-17 09:00:00 {'battery': 14.0, 'rain': 0.0, 'wlevel': 10.2}
            2024-10-17 09:15:00 {'rain': 0.0}
            2024-10-17 09:30:00 {'rain': 0.0}
            2024-10-17 09:45:00 {'rain': 0.0}
            2024-10-17 10:00:00 {'battery': 13.3, 'rain': 0.0, 'wlevel': 10.1}
            2024-10-17 10:15:00 {'rain': 0.0}
            2024-10-17 10:30:00 {'rain': 0.0}
            2024-10-17 10:45:00 {'rain': 0.0}
            2024-10-17 11:00:00 {'battery': 13.2, 'rain': 0.0, 'wlevel': 10.1}        
        '''
        from app.models import Incoming, RDaily, PosMap, OPos
        ids = ['KJc48Vp56VBpjbGdEd5N6V', 'iZPcitEsytdydHaKoGMSbQ', 'hkyy7zy5hXYJdXXKUcTm76']
        ids = ['hkyy7zy5hXYJdXXKUcTm76']
        chann_no = {'1': 'rain', '2': 'battery', '3': 'wlevel'}
        chann_name = {'Rain Fall': 'rain', 'Battery': 'battery', 'Water Level': 'wlevel'}
        all_rec = {}
        for i in ids:
            rec = Incoming.get(Incoming.id==i)
            data = json.loads(rec.body.replace("'", '"'))
            #print()
            for r in data:
                try:
                    field = chann_name[r['channel']]
                except KeyError:
                    field = chann_no[r['channel_no']]
                
                rec_sampling = r['date_time'].replace(' ', 'T')
                this_key = r['name'] + '_' + rec_sampling[0:10]
                new_rec = {rec_sampling: {field: round(float(r['value']), 1)}}
                if this_key in all_rec:
                    existing_rec = all_rec[this_key]
                    if rec_sampling in existing_rec:
                        existing_rec[rec_sampling].update(new_rec[rec_sampling])
                    else:
                        all_rec[this_key].update(new_rec)
                else:
                    all_rec[this_key] = new_rec

        # INSERT or UPDATE Database (RDaily)
        posmaps = dict([(p.nama, p.pos_id) for p in PosMap.select()])
        for k, v in all_rec.items():
            (nama, this_sampling) = k.rsplit('_', 1)
            pos_id = posmaps.get(nama, None)
            
            tipe = '1' if 'rain' in str(list(v.items())[0]) else '2'
            opos, created = OPos.get_or_create(nama=nama, source='SB',
                                        defaults={'tipe':tipe,
                                                'latest_sampling': datetime.datetime.now()})
            
            new_raw = []
            for sam, vv in v.items():
                vv.update({'sampling': sam})
                new_raw.append(vv)
                print(vv)
            
            # mengupdate latest_sampling
            opos.latest_sampling = new_raw[-1].get('sampling')
            
            rd, created = RDaily.get_or_create(source='SB',
                                        nama=nama,
                                        sampling=this_sampling,
                                        defaults={'pos_id': pos_id,
                                                'raw': json.dumps(new_raw)})
            if not created:
                print('RECALL: ', rd.id, rd.nama)
                existing_data = json.loads(rd.raw)
                print(rd.raw)
                existing_sampling = [data['sampling'] for data in existing_data]
                for row in new_raw:
                    if row.get('sampling') not in existing_sampling:
                        existing_data.append(row)
                        print('APPEND: ', row)
                rd.raw = json.dumps(existing_data)
                rd.save()
            else:
                print('CREATED: ', rd.nama, rd.sampling)
    
######################## DEVELOPMENT ONLY #########################

    @app.cli.command('ews-rain')
    def ews_rain(now=datetime.datetime.now()):
        rd = RDaily.select().where(RDaily.sampling==now.date())
        rain_list = []
        for r in rd:
            raw = json.loads(r.raw)
            if 'rain' not in raw[0]:
                continue
            pos = r.nama
            pos_id = None
            if r.pos != None:
                pos = r.pos.nama
                pos_id = r.pos.id
            if 'PDA' in pos:
                continue
            minute_start = datetime.datetime.fromisoformat(raw[-1]['sampling'])
            durasi = datetime.timedelta()
            hujan = 0
            if r.source in ('SA', 'SB'):
                for ra in reversed(raw):
                    sampling = datetime.datetime.fromisoformat(ra['sampling'])
                    if sampling < now - datetime.timedelta(minutes=60):
                        continue
                    if float(ra['rain']) > 0.0:
                        durasi += minute_start - sampling
                        minute_start = sampling
                    hujan += float(ra['rain'])
            else:
                l = raw[-1]['rain']
                for ra in reversed(raw):
                    sampling = datetime.datetime.fromisoformat(ra['sampling'])
                    if sampling < now - datetime.timedelta(minutes=60):
                        continue
                    rain_now = l - ra['rain']
                    if rain_now > 0.0:
                        durasi += minute_start - sampling
                        minute_start = sampling
                    hujan += rain_now
                    l = ra['rain']
                    #click.echo('{} {}'.format(sampling.strftime('%H:%M'), ra['rain']))
            if hujan > 10.0:
                rain_list.append({'pos': pos, 'pos_id': pos_id, 'rain': hujan, 'duration': durasi.total_seconds()})
                try:
                    click.echo('Pos: {} {}'.format(r.pos.nama, len(raw)))
                except:
                    click.echo('{} {}'.format(r.nama, len(raw)))
                click.echo('Hujan: {} Durasi: {}'.format(hujan, durasi))
        msg = ''
        if rain_list:
            msg = '*\[PERINGATAN HUJAN\]*\n*BBWS Citanduy*\n\ndibuat: *{}*\n\n'.format(now.strftime('%d %b %Y jam %H:%M'))
            for i in range(len(rain_list)):
                data = rain_list[i]
                msg += '{}\. {} *{:.0f}mm* \(*{}* menit\)\n'.format(i+1, data['pos'], data['rain'], int(data['duration'] /60))
            msg += '\n\n[Peta Hujan BBWS Citanduy](https://sihka.bbwscitanduy.id/map/hujan)'
            url = 'https://api.telegram.org/bot' + BOT_TOKEN + '/sendMessage?chat_id=' + CTY_OFFICE_ID + '&text=' + msg + '&parse_mode=MarkdownV2'
            resp = requests.get(url)
        click.echo(msg)

                
    @app.cli.command('hello')
    def hello():
        panjalu_11nop = RDaily.get(15813)
        panjalu_12nop = RDaily.get(15888)
        data = [(k, v) for k, v in panjalu_11nop._24jam().items() if k > 6]
        data2 = [(k, v) for k, v in panjalu_12nop._24jam().items() if k < 7]

        data += data2
        rain_before = 0
        rain_jum = 0
        for k, v in data:
            rain_jam = v.get('rain') - rain_before
            print(k, 'rain_jumlah:', v.get('rain'), 'rain_jam:', rain_jam, ' ', v.get('num'))
            rain_before = v.get('rain')
            rain_jum += rain_jam
        print('Rain Hari:', rain_jum)        