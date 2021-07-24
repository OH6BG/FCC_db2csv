import sqlite3
from pathlib import Path
import pygeodesy.dms as dms

# Get data from:
# https://transition.fcc.gov/fcc-bin/amq?call=&arn=&state=&city=&freq=530&fre2=1700&type=0&facid=&class=&list=4

cc = {
    'AC': 'Antigua',
    'AR': 'Argentina',
    'AV': 'Anguilla',
    'BB': 'Barbados',
    'BD': 'Bermuda',
    'BF': 'Bahamas',
    'BH': 'Belize',
    'BL': 'Bolivia',
    'BR': 'Brazil',
    'CA': 'Canada',
    'CI': 'Chile',
    'CJ': 'Cayman Isl',
    'CO': 'Colombia',
    'CS': 'Costa Rica',
    'CU': 'Cuba',
    'DO': 'Dominica',
    'DR': 'Dominican Republic',
    'EC': 'Ecuador',
    'ES': 'El Salvador',
    'FA': 'Falkland Isl',
    'FG': 'French Guiana',
    'GJ': 'Grenada',
    'GL': 'Greenland',
    'GP': 'Guadeloupe',
    'GT': 'Guatemala',
    'GY': 'Guyana',
    'HA': 'Haiti',
    'HO': 'Honduras',
    'JM': 'Jamaica',
    'MB': 'Martinique',
    'MH': 'Montserrat',
    'MX': 'Mexico',
    'NA': 'Netherlands Antilles',
    'NS': 'Suriname',
    'NU': 'Nicaragua',
    'PA': 'Paraguay',
    'PE': 'Peru',
    'PM': 'Panama',
    'SC': 'St Kitts and Nevis',
    'ST': 'St Lucia',
    'TD': 'Trinidad and Tobago',
    'TK': 'Turks and Caicos',
    'US': 'United States',
    'UY': 'Uruguay',
    'VC': 'St Vincent and the Grenadines',
    'VE': 'Venezuela',
    'VI': 'British Virgin Isl',
}

fin = Path.cwd() / "fcc-am-stations.txt"
fout = Path.cwd() / "fcc-am-stations-cleaned.txt"
stations = fin.read_text().splitlines()

# SQLite init
conn = sqlite3.connect('am.db')
c = conn.cursor()
c.execute("""CREATE TABLE IF NOT EXISTS stations (
    frequency integer,
    callsign text,
    city text,
    state text,
    country_code text,
    country_name text,
    power text,
    lat text,
    lon text,
    lat_decimal real,
    lon_decimal real,
    license_holder text
)""")

conn.commit()

# process stations and write to SQLite db
for stn in stations:
    r = stn.split('|')
    call = r[1].strip()
    freq = int(r[2].split()[0].strip())
    day = r[6].strip()
    city = r[10].strip().replace("'", "")
    state = r[11].strip()
    country = r[12].strip()
    country_name = cc[country]

    if country == 'US' and state == 'AK':
        country_name = 'Alaska'
    if country == 'US' and state == 'GU':
        country_name = 'Guam'
    if country == 'US' and state == 'HI':
        country_name = 'Hawaii'
    if country == 'US' and state == 'MP':
        country_name = 'Saipan'
    if country == 'US' and state == 'PR':
        country_name = 'Puerto Rico'
    if country == 'US' and state == 'VI':
        country_name = 'US Virgin Isl'

    power = r[14].split()[0].strip()
    lat = " ".join([r[20].strip(), r[21].strip(),
                   r[22].strip(), r[19].strip()])
    lon = " ".join([r[24].strip(), r[25].strip(),
                   r[26].strip(), r[23].strip()])
    licholder = r[27].strip()
    latdec, londec = dms.parseDMS2(lat, lon)

    sql = ("SELECT rowid, * from stations WHERE frequency = '"
           + str(freq)
           + "' AND callsign = '"
           + call
           + "' AND city = '"
           + city
           + "'")
    c.execute(sql)
    f = c.fetchone()

    if f is None:
        sql1 = "INSERT INTO stations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        c.execute(sql1, (freq,
                         call,
                         city,
                         state,
                         country,
                         country_name,
                         power,
                         lat,
                         lon,
                         latdec,
                         londec,
                         licholder
                         )
                  )
    else:
        # consolidate entries of same stations, using different power
        pwr = (f[7] + "/" + power).split("/")
        # remove dupes, and maintain the order
        pwr = list(dict.fromkeys(pwr))
        pwr = "/".join(pwr)

        sql2 = ("UPDATE stations SET power = '"
                + pwr
                + "' WHERE rowid = "
                + str(f[0]))
        c.execute(sql2)

conn.commit()

# show all db entries on screen
c.execute("SELECT rowid, * FROM stations")
items = c.fetchall()
fo = "frequency,callsign,city,state,country_code,country_name,power,lat,lon,lat_decimal,lon_decimal,license_holder\n"

for i in items:
    print(f"{i[0]:>5} {i[1]:>4} {i[2]:>10} {i[3][:20]:<20} {i[4]:>2} {i[5]:>2} {i[6][:20]:<20} {i[7]:<20}")
    fo += f"{i[1]},{i[2]},{i[3]},{i[4]},{i[5]},{i[6]},{i[7]},{i[8]},{i[9]},{i[10]:.6f},{i[11]:.6f},{i[12]}\n"

# close connection to db
conn.close()
fout.write_text(fo)
print(f"Completed writing to file: {fout}")
