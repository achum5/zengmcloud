"""Build src/common/cityLocations.ts: the cities a team can be placed in.

Data, downloaded and only read:
- https://raw.githubusercontent.com/plotly/datasets/master/2014_us_cities.csv
  (US city names, populations and coordinates)
- https://raw.githubusercontent.com/lutangar/cities.json/master/cities.json
  (GeoNames cities1000, CC BY 4.0: names, coordinates, country, state)

Run: python3 -I tools/cityLocations/build.py <dir with both files> src/common/cityLocations.ts
"""

import csv
import json
import math
import sys
import unicodedata

data_dir, out_path = sys.argv[1], sys.argv[2]

geo = json.load(open(f"{data_dir}/cities1000.json"))
by_name_country = {}
for c in geo:
    by_name_country.setdefault((c["name"], c["country"]), []).append(c)


def miles(a, b):
    la1, lo1, la2, lo2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    h = (
        math.sin((la2 - la1) / 2) ** 2
        + math.cos(la1) * math.cos(la2) * math.sin((lo2 - lo1) / 2) ** 2
    )
    return 3959 * 2 * math.asin(min(1, math.sqrt(h)))


cities = {}  # display name -> (lat, lon)
problems = []

# 1. US cities of about 90,000 or more, the biggest of each name.
RENAME = {
    "Nashville-Davidson": "Nashville",
    "Louisville/Jefferson County": "Louisville",
    "Lexington-Fayette urban county": "Lexington",
    "Boise City": "Boise",
    "Macon-Bibb County": "Macon",
    "San Buenaventura (Ventura)": "Ventura",
}
best = {}
for row in csv.DictReader(open(f"{data_dir}/us_cities_2014.csv")):
    name = row["name"].strip()
    name = RENAME.get(name, name)
    pop = int(row["pop"])
    if pop < 90000:
        continue
    if name not in best or pop > best[name][0]:
        best[name] = (pop, float(row["lat"]), float(row["lon"]))
for name, (pop, lat, lon) in best.items():
    cities[name] = (lat, lon)

# The plain name goes to the city people mean in basketball, where plotly's
# geocoding picked another.
US_PLAIN = {"Springfield": "MA", "Columbia": "SC", "Henderson": "NV", "Warren": "MI", "Frisco": "TX", "Allen": "TX"}
for name, state in US_PLAIN.items():
    found = [c for c in by_name_country.get((name, "US"), []) if c["admin1"] == state]
    cities[name] = (float(found[0]["lat"]), float(found[0]["lng"]))

# Every US city should sit on a GeoNames city of its name.
for name, (lat, lon) in list(cities.items()):
    cands = by_name_country.get((name, "US"), [])
    if not cands and name.startswith("St. "):
        cands = by_name_country.get(("Saint " + name[4:], "US"), [])
    d = min((miles((lat, lon), (float(c["lat"]), float(c["lng"]))) for c in cands), default=1e9)
    if d > 25:
        problems.append(f"US check: {name} is {d:.0f} mi from any GeoNames {name}")

# 2. Smaller US cities with a team history or a team-sized market, by state.
US_EXTRA = [
    ("Birmingham", "AL"), ("Anchorage", "AK"), ("Augusta", "GA"), ("Montgomery", "AL"),
    ("Mobile", "AL"), ("Little Rock", "AR"), ("Columbia", "SC"), ("Honolulu", "HI"),
    ("Louisville", "KY"), ("Nashville", "TN"), ("Boise", "ID"), ("Lexington", "KY"),
    ("Green Bay", "WI"), ("Sheboygan", "WI"), ("Oshkosh", "WI"), ("Fort Wayne", "IN"),
    ("Anderson", "IN"), ("South Bend", "IN"), ("Waterloo", "IA"), ("Moline", "IL"),
    ("Rock Island", "IL"), ("Peoria", "IL"), ("Syracuse", "NY"), ("Albany", "NY"),
    ("Utica", "NY"), ("Binghamton", "NY"), ("Erie", "PA"), ("Scranton", "PA"),
    ("Allentown", "PA"), ("Harrisburg", "PA"), ("Reading", "PA"), ("Trenton", "NJ"),
    ("Atlantic City", "NJ"), ("Wilmington", "DE"), ("Hartford", "CT"), ("New Haven", "CT"),
    ("Bridgeport", "CT"), ("Worcester", "MA"), ("Portland", "ME"), ("Bangor", "ME"),
    ("Manchester", "NH"), ("Burlington", "VT"), ("Charleston", "SC"), ("Charleston", "WV"),
    ("Greenville", "SC"), ("Savannah", "GA"), ("Macon", "GA"), ("Asheville", "NC"),
    ("Roanoke", "VA"), ("Richmond", "VA"), ("Norfolk", "VA"), ("Huntington", "WV"),
    ("Dayton", "OH"), ("Canton", "OH"), ("Youngstown", "OH"), ("Flint", "MI"),
    ("Saginaw", "MI"), ("Kalamazoo", "MI"), ("Grand Rapids", "MI"), ("Lansing", "MI"),
    ("Biloxi", "MS"), ("Jackson", "MS"), ("Shreveport", "LA"), ("Lafayette", "LA"),
    ("Pensacola", "FL"), ("Tallahassee", "FL"), ("Fort Myers", "FL"), ("Sarasota", "FL"),
    ("Daytona Beach", "FL"), ("Key West", "FL"), ("Duluth", "MN"), ("Bismarck", "ND"),
    ("Fargo", "ND"), ("Rapid City", "SD"), ("Sioux City", "IA"), ("Davenport", "IA"),
    ("Cedar Rapids", "IA"), ("Billings", "MT"), ("Missoula", "MT"), ("Casper", "WY"),
    ("Cheyenne", "WY"), ("Santa Fe", "NM"), ("Las Cruces", "NM"), ("Flagstaff", "AZ"),
    ("Yakima", "WA"), ("Bellingham", "WA"), ("Eugene", "OR"), ("Salem", "OR"),
    ("Bend", "OR"), ("Medford", "OR"), ("Santa Cruz", "CA"), ("Santa Barbara", "CA"),
    ("Palm Springs", "CA"), ("Monterey", "CA"), ("Redding", "CA"), ("Juneau", "AK"),
    ("Fairbanks", "AK"), ("Hilo", "HI"), ("Lake Charles", "LA"), ("Texarkana", "TX"),
    ("Galveston", "TX"), ("Odessa", "TX"), ("Midland", "TX"), ("Lubbock", "TX"),
    ("Amarillo", "TX"), ("Abilene", "TX"), ("Wichita Falls", "TX"), ("Tyler", "TX"),
    ("Topeka", "KS"), ("Lawrence", "KS"), ("Columbia", "MO"), ("St. Joseph", "MO"),
    ("Evansville", "IN"), ("Terre Haute", "IN"), ("Bloomington", "IN"), ("Champaign", "IL"),
    ("Murray", "KY"), ("Bowling Green", "KY"), ("Owensboro", "KY"), ("Ann Arbor", "MI"),
    ("State College", "PA"), ("Ithaca", "NY"), ("Durham", "NC"), ("Chapel Hill", "NC"),
    ("Annapolis", "MD"), ("Dover", "DE"), ("Ogden", "UT"), ("Provo", "UT"),
    ("Pocatello", "ID"), ("Twin Falls", "ID"), ("Elko", "NV"), ("Carson City", "NV"),
]
for name, state in US_EXTRA:
    found = [c for c in by_name_country.get((name, "US"), []) if c["admin1"] == state]
    if not found and name.startswith("St. "):
        found = [
            c
            for c in by_name_country.get(("Saint " + name[4:], "US"), [])
            if c["admin1"] == state
        ]
    if not found:
        problems.append(f"US extra not found: {name}, {state}")
        continue
    key = name
    if key in cities:
        # Keep the bigger one (plain "Charleston" is South Carolina's).
        continue
    cities[key] = (float(found[0]["lat"]), float(found[0]["lng"]))

# 3. The rest of the world: approximate coordinates, snapped to GeoNames.
WORLD = [
    # Canada
    ("Toronto", "CA", 43.65, -79.38), ("Montreal", "CA", 45.50, -73.57),
    ("Vancouver", "CA", 49.28, -123.12), ("Calgary", "CA", 51.05, -114.07),
    ("Edmonton", "CA", 53.55, -113.49), ("Ottawa", "CA", 45.42, -75.70),
    ("Winnipeg", "CA", 49.90, -97.14), ("Quebec City", "CA", 46.81, -71.21),
    ("Hamilton", "CA", 43.26, -79.87), ("Halifax", "CA", 44.65, -63.57),
    ("Victoria", "CA", 48.43, -123.37), ("Saskatoon", "CA", 52.13, -106.67),
    ("Regina", "CA", 50.45, -104.62), ("Kitchener", "CA", 43.45, -80.49),
    ("Windsor", "CA", 42.32, -83.04), ("St. John's", "CA", 47.56, -52.71),
    ("Mississauga", "CA", 43.59, -79.64), ("Brampton", "CA", 43.73, -79.76),
    ("Surrey", "CA", 49.19, -122.85), ("Kelowna", "CA", 49.89, -119.50),
    ("Moncton", "CA", 46.09, -64.78), ("Sudbury", "CA", 46.49, -80.99),
    ("Thunder Bay", "CA", 48.38, -89.25), ("Laval", "CA", 45.57, -73.69),
    ("Gatineau", "CA", 45.48, -75.70), ("Saint John", "CA", 45.27, -66.06),
    ("Charlottetown", "CA", 46.24, -63.13), ("Whitehorse", "CA", 60.72, -135.06),
    ("Yellowknife", "CA", 62.45, -114.37), ("Kingston", "CA", 44.23, -76.49),
    # Mexico, Central America, Caribbean
    ("Mexico City", "MX", 19.43, -99.13), ("Guadalajara", "MX", 20.67, -103.35),
    ("Monterrey", "MX", 25.67, -100.31), ("Tijuana", "MX", 32.51, -117.04),
    ("Puebla", "MX", 19.04, -98.21), ("Cancun", "MX", 21.16, -86.85),
    ("Leon", "MX", 21.12, -101.68), ("Juarez", "MX", 31.69, -106.42),
    ("Merida", "MX", 20.97, -89.62), ("Chihuahua", "MX", 28.63, -106.07),
    ("Hermosillo", "MX", 29.07, -110.96), ("Mexicali", "MX", 32.62, -115.45),
    ("Culiacan", "MX", 24.80, -107.39), ("Mazatlan", "MX", 23.22, -106.42),
    ("Acapulco", "MX", 16.86, -99.88), ("Veracruz", "MX", 19.18, -96.14),
    ("Queretaro", "MX", 20.59, -100.39), ("Aguascalientes", "MX", 21.88, -102.29),
    ("San Luis Potosi", "MX", 22.15, -100.98), ("Saltillo", "MX", 25.42, -101.00),
    ("Torreon", "MX", 25.54, -103.42), ("Oaxaca", "MX", 17.06, -96.73),
    ("Havana", "CU", 23.13, -82.38), ("San Juan", "PR", 18.47, -66.11),
    ("Santo Domingo", "DO", 18.48, -69.93), ("Kingston", "JM", 17.99, -76.79),
    ("Port-au-Prince", "HT", 18.54, -72.34), ("Nassau", "BS", 25.06, -77.35),
    ("Panama City", "PA", 8.98, -79.52), ("San Jose", "CR", 9.93, -84.08),
    ("Guatemala City", "GT", 14.63, -90.51), ("San Salvador", "SV", 13.69, -89.19),
    ("Tegucigalpa", "HN", 14.07, -87.19), ("Managua", "NI", 12.13, -86.25),
    ("Port of Spain", "TT", 10.65, -61.51), ("Bridgetown", "BB", 13.10, -59.62),
    # South America
    ("Buenos Aires", "AR", -34.60, -58.38), ("Cordoba", "AR", -31.42, -64.18),
    ("Rosario", "AR", -32.95, -60.65), ("Mar del Plata", "AR", -38.00, -57.56),
    ("Bahia Blanca", "AR", -38.72, -62.27), ("Mendoza", "AR", -32.89, -68.83),
    ("Sao Paulo", "BR", -23.55, -46.63), ("Rio de Janeiro", "BR", -22.91, -43.17),
    ("Brasilia", "BR", -15.79, -47.88), ("Belo Horizonte", "BR", -19.92, -43.94),
    ("Salvador", "BR", -12.97, -38.50), ("Fortaleza", "BR", -3.73, -38.52),
    ("Recife", "BR", -8.05, -34.88), ("Porto Alegre", "BR", -30.03, -51.23),
    ("Curitiba", "BR", -25.43, -49.27), ("Manaus", "BR", -3.12, -60.02),
    ("Franca", "BR", -20.54, -47.40), ("Santiago", "CL", -33.45, -70.67),
    ("Valparaiso", "CL", -33.05, -71.62), ("Lima", "PE", -12.05, -77.04),
    ("Bogota", "CO", 4.71, -74.07), ("Medellin", "CO", 6.25, -75.56),
    ("Cali", "CO", 3.45, -76.53), ("Barranquilla", "CO", 10.96, -74.80),
    ("Caracas", "VE", 10.49, -66.88), ("Maracaibo", "VE", 10.65, -71.64),
    ("Quito", "EC", -0.18, -78.47), ("Guayaquil", "EC", -2.19, -79.89),
    ("Montevideo", "UY", -34.90, -56.16), ("Asuncion", "PY", -25.26, -57.58),
    ("La Paz", "BO", -16.50, -68.15), ("Santa Cruz de la Sierra", "BO", -17.78, -63.18),
    # Europe
    ("London", "GB", 51.51, -0.13), ("Manchester", "GB", 53.48, -2.24),
    ("Liverpool", "GB", 53.41, -2.98), ("Leeds", "GB", 53.80, -1.55),
    ("Newcastle upon Tyne", "GB", 54.98, -1.61), ("Sheffield", "GB", 53.38, -1.47),
    ("Bristol", "GB", 51.45, -2.59), ("Nottingham", "GB", 52.95, -1.15),
    ("Leicester", "GB", 52.64, -1.13), ("Glasgow", "GB", 55.86, -4.25),
    ("Edinburgh", "GB", 55.95, -3.19), ("Cardiff", "GB", 51.48, -3.18),
    ("Belfast", "GB", 54.60, -5.93), ("Dublin", "IE", 53.35, -6.26),
    ("Cork", "IE", 51.90, -8.47), ("Paris", "FR", 48.86, 2.35),
    ("Lyon", "FR", 45.76, 4.84), ("Marseille", "FR", 43.30, 5.37),
    ("Toulouse", "FR", 43.60, 1.44), ("Nice", "FR", 43.70, 7.27),
    ("Nantes", "FR", 47.22, -1.55), ("Strasbourg", "FR", 48.57, 7.75),
    ("Bordeaux", "FR", 44.84, -0.58), ("Lille", "FR", 50.63, 3.06),
    ("Le Mans", "FR", 48.00, 0.20), ("Limoges", "FR", 45.83, 1.26),
    ("Pau", "FR", 43.30, -0.37), ("Villeurbanne", "FR", 45.77, 4.88),
    ("Monaco", "MC", 43.73, 7.42), ("Madrid", "ES", 40.42, -3.70),
    ("Barcelona", "ES", 41.39, 2.17), ("Valencia", "ES", 39.47, -0.38),
    ("Seville", "ES", 37.39, -5.98), ("Malaga", "ES", 36.72, -4.42),
    ("Bilbao", "ES", 43.26, -2.93), ("Vitoria-Gasteiz", "ES", 42.85, -2.67),
    ("Zaragoza", "ES", 41.65, -0.89), ("Murcia", "ES", 37.99, -1.13),
    ("Palma", "ES", 39.57, 2.65), ("Las Palmas", "ES", 28.10, -15.41),
    ("Granada", "ES", 37.18, -3.60), ("Badalona", "ES", 41.45, 2.25),
    ("Lisbon", "PT", 38.72, -9.14), ("Porto", "PT", 41.15, -8.61),
    ("Berlin", "DE", 52.52, 13.40), ("Munich", "DE", 48.14, 11.58),
    ("Hamburg", "DE", 53.55, 9.99), ("Frankfurt", "DE", 50.11, 8.68),
    ("Cologne", "DE", 50.94, 6.96), ("Stuttgart", "DE", 48.78, 9.18),
    ("Dusseldorf", "DE", 51.23, 6.78), ("Dortmund", "DE", 51.51, 7.47),
    ("Leipzig", "DE", 51.34, 12.37), ("Bamberg", "DE", 49.89, 10.89),
    ("Ulm", "DE", 48.40, 9.99), ("Oldenburg", "DE", 53.14, 8.21),
    ("Bonn", "DE", 50.74, 7.10), ("Amsterdam", "NL", 52.37, 4.90),
    ("Rotterdam", "NL", 51.92, 4.48), ("The Hague", "NL", 52.08, 4.30),
    ("Brussels", "BE", 50.85, 4.35), ("Antwerp", "BE", 51.22, 4.40),
    ("Ostend", "BE", 51.22, 2.92), ("Luxembourg", "LU", 49.61, 6.13),
    ("Zurich", "CH", 47.38, 8.54), ("Geneva", "CH", 46.20, 6.14),
    ("Vienna", "AT", 48.21, 16.37), ("Milan", "IT", 45.46, 9.19),
    ("Rome", "IT", 41.90, 12.50), ("Bologna", "IT", 44.49, 11.34),
    ("Venice", "IT", 45.44, 12.33), ("Turin", "IT", 45.07, 7.69),
    ("Naples", "IT", 40.85, 14.27), ("Florence", "IT", 43.77, 11.26),
    ("Sassari", "IT", 40.73, 8.56), ("Varese", "IT", 45.82, 8.83),
    ("Trento", "IT", 46.07, 11.12), ("Brescia", "IT", 45.54, 10.22),
    ("Athens", "GR", 37.98, 23.73), ("Thessaloniki", "GR", 40.64, 22.94),
    ("Piraeus", "GR", 37.94, 23.65), ("Istanbul", "TR", 41.01, 28.98),
    ("Ankara", "TR", 39.93, 32.86), ("Izmir", "TR", 38.42, 27.14),
    ("Bursa", "TR", 40.19, 29.06), ("Belgrade", "RS", 44.79, 20.45),
    ("Novi Sad", "RS", 45.27, 19.83), ("Zagreb", "HR", 45.81, 15.98),
    ("Split", "HR", 43.51, 16.44), ("Ljubljana", "SI", 46.06, 14.51),
    ("Sarajevo", "BA", 43.86, 18.41), ("Podgorica", "ME", 42.44, 19.26),
    ("Skopje", "MK", 41.99, 21.43), ("Tirana", "AL", 41.33, 19.82),
    ("Sofia", "BG", 42.70, 23.32), ("Bucharest", "RO", 44.43, 26.10),
    ("Cluj-Napoca", "RO", 46.77, 23.60), ("Budapest", "HU", 47.50, 19.04),
    ("Prague", "CZ", 50.08, 14.44), ("Brno", "CZ", 49.20, 16.61),
    ("Bratislava", "SK", 48.15, 17.11), ("Warsaw", "PL", 52.23, 21.01),
    ("Krakow", "PL", 50.06, 19.94), ("Gdansk", "PL", 54.35, 18.65),
    ("Wroclaw", "PL", 51.11, 17.03), ("Vilnius", "LT", 54.69, 25.28),
    ("Kaunas", "LT", 54.90, 23.90), ("Riga", "LV", 56.95, 24.11),
    ("Tallinn", "EE", 59.44, 24.75), ("Helsinki", "FI", 60.17, 24.94),
    ("Stockholm", "SE", 59.33, 18.07), ("Gothenburg", "SE", 57.71, 11.97),
    ("Oslo", "NO", 59.91, 10.75), ("Copenhagen", "DK", 55.68, 12.57),
    ("Reykjavik", "IS", 64.15, -21.94), ("Moscow", "RU", 55.76, 37.62),
    ("Saint Petersburg", "RU", 59.94, 30.31), ("Kazan", "RU", 55.79, 49.12),
    ("Yekaterinburg", "RU", 56.84, 60.61), ("Novosibirsk", "RU", 55.03, 82.92),
    ("Kyiv", "UA", 50.45, 30.52), ("Kharkiv", "UA", 49.99, 36.23),
    ("Odesa", "UA", 46.48, 30.73), ("Minsk", "BY", 53.90, 27.56),
    ("Chisinau", "MD", 47.01, 28.86), ("Tbilisi", "GE", 41.72, 44.79),
    ("Yerevan", "AM", 40.18, 44.51), ("Baku", "AZ", 40.41, 49.87),
    ("Valletta", "MT", 35.90, 14.51), ("Nicosia", "CY", 35.17, 33.36),
    # Middle East and Africa
    ("Tel Aviv", "IL", 32.09, 34.78), ("Jerusalem", "IL", 31.77, 35.21),
    ("Haifa", "IL", 32.79, 34.99), ("Beirut", "LB", 33.89, 35.50),
    ("Amman", "JO", 31.95, 35.93), ("Damascus", "SY", 33.51, 36.29),
    ("Baghdad", "IQ", 33.31, 44.37), ("Tehran", "IR", 35.69, 51.39),
    ("Riyadh", "SA", 24.71, 46.68), ("Jeddah", "SA", 21.49, 39.19),
    ("Dubai", "AE", 25.20, 55.27), ("Abu Dhabi", "AE", 24.45, 54.38),
    ("Doha", "QA", 25.29, 51.53), ("Kuwait City", "KW", 29.38, 47.99),
    ("Manama", "BH", 26.23, 50.59), ("Muscat", "OM", 23.59, 58.41),
    ("Cairo", "EG", 30.04, 31.24), ("Alexandria", "EG", 31.20, 29.92),
    ("Casablanca", "MA", 33.57, -7.59), ("Rabat", "MA", 34.02, -6.83),
    ("Tunis", "TN", 36.81, 10.18), ("Algiers", "DZ", 36.75, 3.06),
    ("Tripoli", "LY", 32.89, 13.19), ("Lagos", "NG", 6.52, 3.38),
    ("Abuja", "NG", 9.08, 7.40), ("Accra", "GH", 5.60, -0.19),
    ("Dakar", "SN", 14.72, -17.47), ("Abidjan", "CI", 5.35, -4.01),
    ("Kinshasa", "CD", -4.44, 15.27), ("Luanda", "AO", -8.84, 13.23),
    ("Nairobi", "KE", -1.29, 36.82), ("Addis Ababa", "ET", 9.03, 38.74),
    ("Kampala", "UG", 0.35, 32.58), ("Kigali", "RW", -1.95, 30.06),
    ("Dar es Salaam", "TZ", -6.79, 39.21), ("Johannesburg", "ZA", -26.20, 28.05),
    ("Cape Town", "ZA", -33.92, 18.42), ("Durban", "ZA", -29.86, 31.02),
    ("Pretoria", "ZA", -25.75, 28.19), ("Harare", "ZW", -17.83, 31.05),
    ("Maputo", "MZ", -25.97, 32.57), ("Yaounde", "CM", 3.87, 11.52),
    ("Douala", "CM", 4.05, 9.70), ("Bamako", "ML", 12.64, -8.00),
    ("Antananarivo", "MG", -18.88, 47.51),
    # Asia and Oceania
    ("Tokyo", "JP", 35.68, 139.69), ("Osaka", "JP", 34.69, 135.50),
    ("Yokohama", "JP", 35.44, 139.64), ("Nagoya", "JP", 35.18, 136.91),
    ("Sapporo", "JP", 43.06, 141.35), ("Fukuoka", "JP", 33.59, 130.40),
    ("Kyoto", "JP", 35.01, 135.77), ("Okinawa", "JP", 26.33, 127.80),
    ("Chiba", "JP", 35.61, 140.12), ("Seoul", "KR", 37.57, 126.98),
    ("Busan", "KR", 35.18, 129.08), ("Incheon", "KR", 37.46, 126.71),
    ("Daegu", "KR", 35.87, 128.60), ("Beijing", "CN", 39.90, 116.41),
    ("Shanghai", "CN", 31.23, 121.47), ("Guangzhou", "CN", 23.13, 113.26),
    ("Shenzhen", "CN", 22.54, 114.06), ("Chengdu", "CN", 30.57, 104.07),
    ("Chongqing", "CN", 29.56, 106.55), ("Wuhan", "CN", 30.59, 114.31),
    ("Xi'an", "CN", 34.34, 108.94), ("Hangzhou", "CN", 30.27, 120.16),
    ("Nanjing", "CN", 32.06, 118.80), ("Tianjin", "CN", 39.34, 117.36),
    ("Shenyang", "CN", 41.81, 123.43), ("Qingdao", "CN", 36.07, 120.38),
    ("Dongguan", "CN", 23.02, 113.75), ("Hong Kong", "HK", 22.32, 114.17),
    ("Macau", "MO", 22.20, 113.54), ("Taipei", "TW", 25.03, 121.57),
    ("Kaohsiung", "TW", 22.63, 120.30), ("Taichung", "TW", 24.15, 120.67),
    ("Ulaanbaatar", "MN", 47.89, 106.91), ("Manila", "PH", 14.60, 120.98),
    ("Quezon City", "PH", 14.68, 121.04), ("Cebu City", "PH", 10.32, 123.89),
    ("Davao", "PH", 7.19, 125.46), ("Jakarta", "ID", -6.21, 106.85),
    ("Surabaya", "ID", -7.25, 112.75), ("Bandung", "ID", -6.92, 107.61),
    ("Bali", "ID", -8.65, 115.22), ("Singapore", "SG", 1.35, 103.82),
    ("Kuala Lumpur", "MY", 3.14, 101.69), ("Bangkok", "TH", 13.76, 100.50),
    ("Hanoi", "VN", 21.03, 105.85), ("Ho Chi Minh City", "VN", 10.82, 106.63),
    ("Phnom Penh", "KH", 11.56, 104.92), ("Yangon", "MM", 16.87, 96.20),
    ("Delhi", "IN", 28.70, 77.10), ("Mumbai", "IN", 19.08, 72.88),
    ("Bangalore", "IN", 12.97, 77.59), ("Chennai", "IN", 13.08, 80.27),
    ("Kolkata", "IN", 22.57, 88.36), ("Hyderabad", "IN", 17.39, 78.49),
    ("Karachi", "PK", 24.86, 67.01), ("Lahore", "PK", 31.55, 74.34),
    ("Islamabad", "PK", 33.68, 73.05), ("Dhaka", "BD", 23.81, 90.41),
    ("Kathmandu", "NP", 27.72, 85.32), ("Colombo", "LK", 6.93, 79.86),
    ("Kabul", "AF", 34.56, 69.21), ("Tashkent", "UZ", 41.30, 69.24),
    ("Almaty", "KZ", 43.24, 76.89), ("Astana", "KZ", 51.17, 71.45),
    ("Sydney", "AU", -33.87, 151.21), ("Melbourne", "AU", -37.81, 144.96),
    ("Brisbane", "AU", -27.47, 153.03), ("Perth", "AU", -31.95, 115.86),
    ("Adelaide", "AU", -34.93, 138.60), ("Gold Coast", "AU", -28.02, 153.40),
    ("Canberra", "AU", -35.28, 149.13), ("Hobart", "AU", -42.88, 147.33),
    ("Darwin", "AU", -12.46, 130.84), ("Cairns", "AU", -16.92, 145.77),
    ("Townsville", "AU", -19.26, 146.82), ("Auckland", "NZ", -36.85, 174.76),
    ("Wellington", "NZ", -41.29, 174.78), ("Christchurch", "NZ", -43.53, 172.64),
    ("Honolulu", "US", 21.31, -157.86), ("Suva", "FJ", -18.14, 178.44),
    ("Port Moresby", "PG", -9.44, 147.18), ("Guam", "GU", 13.44, 144.79),
]
ALT_NAMES = {
    "Kyiv": ["Kyiv", "Kiev"],
    "Odesa": ["Odesa", "Odessa"],
    "Quebec City": ["Québec", "Quebec"],
    "Montreal": ["Montréal", "Montreal"],
    "Bogota": ["Bogotá"],
    "Medellin": ["Medellín"],
    "Sao Paulo": ["São Paulo"],
    "Brasilia": ["Brasília"],
    "Belo Horizonte": ["Belo Horizonte"],
    "Asuncion": ["Asunción"],
    "Valparaiso": ["Valparaíso"],
    "Malaga": ["Málaga"],
    "Zurich": ["Zürich"],
    "Dusseldorf": ["Düsseldorf"],
    "Cologne": ["Köln", "Cologne"],
    "Munich": ["München", "Munich"],
    "Rome": ["Rome", "Roma"],
    "Milan": ["Milan", "Milano"],
    "Naples": ["Naples", "Napoli"],
    "Turin": ["Turin", "Torino"],
    "Florence": ["Florence", "Firenze"],
    "Venice": ["Venice", "Venezia"],
    "Lisbon": ["Lisbon", "Lisboa"],
    "Seville": ["Sevilla", "Seville"],
    "Prague": ["Prague", "Praha"],
    "Warsaw": ["Warsaw", "Warszawa"],
    "Krakow": ["Kraków", "Krakow"],
    "Wroclaw": ["Wrocław", "Wroclaw"],
    "Gdansk": ["Gdańsk", "Gdansk"],
    "Bucharest": ["Bucharest", "București"],
    "Athens": ["Athens", "Athína"],
    "Thessaloniki": ["Thessaloníki", "Thessaloniki"],
    "Piraeus": ["Piraeus", "Peiraiás"],
    "Belgrade": ["Belgrade", "Beograd"],
    "Copenhagen": ["Copenhagen", "København"],
    "Gothenburg": ["Gothenburg", "Göteborg"],
    "Reykjavik": ["Reykjavík", "Reykjavik"],
    "Moscow": ["Moscow", "Moskva"],
    "Saint Petersburg": ["Saint Petersburg", "Sankt-Peterburg"],
    "Kharkiv": ["Kharkiv", "Kharkov"],
    "Chisinau": ["Chişinău", "Chisinau"],
    "Tel Aviv": ["Tel Aviv", "Tel Aviv-Yafo"],
    "Kuwait City": ["Kuwait City", "Kuwait"],
    "Mexico City": ["Mexico City", "Ciudad de México"],
    "Juarez": ["Ciudad Juárez", "Juárez"],
    "Leon": ["León de los Aldama", "León"],
    "Merida": ["Mérida"],
    "Cancun": ["Cancún"],
    "Queretaro": ["Santiago de Querétaro", "Querétaro"],
    "San Luis Potosi": ["San Luis Potosí"],
    "Torreon": ["Torreón"],
    "Culiacan": ["Culiacán"],
    "Mazatlan": ["Mazatlán"],
    "Oaxaca": ["Oaxaca de Juárez", "Oaxaca"],
    "Yaounde": ["Yaoundé"],
    "Ho Chi Minh City": ["Ho Chi Minh City"],
    "Bangalore": ["Bengaluru", "Bangalore"],
    "Kolkata": ["Kolkata"],
    "Delhi": ["Delhi", "New Delhi"],
    "Xi'an": ["Xi'an", "Xi’an"],
    "Cebu City": ["Cebu City"],
    "Davao": ["Davao"],
    "Bali": ["Denpasar"],
    "Okinawa": ["Okinawa", "Naha"],
    "Port-au-Prince": ["Port-au-Prince"],
    "Ulaanbaatar": ["Ulan Bator", "Ulaanbaatar"],
    "Newcastle upon Tyne": ["Newcastle upon Tyne"],
    "Vitoria-Gasteiz": ["Vitoria-Gasteiz"],
    "Las Palmas": ["Las Palmas de Gran Canaria"],
    "Palma": ["Palma"],
    "Santa Cruz de la Sierra": ["Santa Cruz de la Sierra"],
    "Cluj-Napoca": ["Cluj-Napoca"],
    "Astana": ["Astana", "Nur-Sultan"],
    "Guam": ["Hagåtña", "Dededo"],
    "Macau": ["Macau", "Macao"],
    "Hong Kong": ["Hong Kong"],
    "Kingston": ["Kingston"],
}

for name, country, lat, lon in WORLD:
    candidates = []
    for alt in ALT_NAMES.get(name, [name]) + [name]:
        candidates += by_name_country.get((alt, country), [])
    best_c = None
    best_d = 1e9
    for c in candidates:
        d = miles((lat, lon), (float(c["lat"]), float(c["lng"])))
        if d < best_d:
            best_d, best_c = d, c
    key = name
    if (name, country) == ("Kingston", "CA"):
        key = "Kingston, Ontario"
    if (name, country) == ("San Jose", "CR"):
        key = "San Jose, Costa Rica"
    if best_c is not None and best_d < 40:
        coords = (float(best_c["lat"]), float(best_c["lng"]))
    else:
        problems.append(f"world approx kept: {name} {country} (nearest {best_d:.0f} mi)")
        coords = (lat, lon)
    if key in cities and country != "US":
        # A US city of the same name wins the plain name.
        continue
    cities[key] = coords

# 4. Names BBGM uses that are states, nicknames or old team regions.
ALIASES = {
    "Golden State": "San Francisco", "Utah": "Salt Lake City", "Indiana": "Indianapolis",
    "Minnesota": "Minneapolis", "Brooklyn": "New York", "New Jersey": "Newark",
    "Carolina": "Charlotte", "Kentucky": "Louisville", "Virginia": "Norfolk",
    "Hawaii": "Honolulu", "Capital": "Washington", "Kansas City-Omaha": "Kansas City",
    "Tri-Cities": "Moline", "Texas": "Dallas", "Arizona": "Phoenix", "Colorado": "Denver",
    "Florida": "Miami", "Floridians": "Miami", "Tennessee": "Nashville",
    "Oregon": "Portland", "Alaska": "Anchorage", "Nevada": "Las Vegas", "Iowa": "Des Moines",
    "Nebraska": "Omaha", "Kansas": "Wichita", "Oklahoma": "Oklahoma City",
    "Arkansas": "Little Rock", "Louisiana": "New Orleans", "Alabama": "Birmingham",
    "Mississippi": "Jackson", "New Mexico": "Albuquerque", "Maine": "Portland, Maine",
    "Vermont": "Burlington", "Connecticut": "Hartford", "Rhode Island": "Providence",
    "Delaware": "Wilmington", "Maryland": "Baltimore", "New Hampshire": "Manchester, New Hampshire",
    "Massachusetts": "Boston", "New England": "Boston", "Wisconsin": "Milwaukee",
    "Michigan": "Detroit", "Ohio": "Columbus", "Pennsylvania": "Philadelphia",
    "West Virginia": "Charleston, West Virginia", "South Carolina": "Columbia, South Carolina",
    "North Carolina": "Charlotte", "North Dakota": "Fargo", "South Dakota": "Sioux Falls",
    "Montana": "Billings", "Wyoming": "Cheyenne", "Idaho": "Boise", "Illinois": "Chicago",
    "Missouri": "St. Louis", "Georgia": "Atlanta", "California": "Los Angeles",
    "Washington State": "Seattle", "Bay Area": "San Francisco", "Twin Cities": "Minneapolis",
    "Silicon Valley": "San Jose", "Inland Empire": "Riverside", "Long Island": "Uniondale",
    "Quebec": "Quebec City", "Ontario": "Toronto", "British Columbia": "Vancouver",
    "Alberta": "Edmonton", "Manitoba": "Winnipeg", "Saskatchewan": "Regina",
    "Nova Scotia": "Halifax", "Puerto Rico": "San Juan", "DC": "Washington",
    "Washington DC": "Washington", "Washington, D.C.": "Washington", "LA": "Los Angeles",
    "NYC": "New York", "New York City": "New York", "Philly": "Philadelphia",
    "Vegas": "Las Vegas", "Nola": "New Orleans", "Frisco": "San Francisco",
    "Saint Louis": "St. Louis", "Spirits of St. Louis": "St. Louis",
    "Fort Worth": "Fort Worth", "Dallas-Fort Worth": "Dallas", "Anaheim": "Anaheim",
    "Rochester": "Rochester", "Sheboygan": "Sheboygan", "Waterloo": "Waterloo",
    "Mexico": "Mexico City", "Monterrey": "Monterrey", "Beijing": "Beijing",
    "Ho Chi Minh": "Ho Chi Minh City", "Saigon": "Ho Chi Minh City",
    "Bombay": "Mumbai", "Calcutta": "Kolkata", "Peking": "Beijing", "Kiev": "Kyiv",
    "St. Petersburg": "Saint Petersburg", "Quebec City": "Quebec City",
}

# Disambiguated duplicates kept under qualified names.
QUALIFIED = [
    ("Portland, Maine", "Portland", "ME"), ("Manchester, New Hampshire", "Manchester", "NH"),
    ("Charleston, West Virginia", "Charleston", "WV"), ("Columbia, South Carolina", "Columbia", "SC"),
    ("Columbia, Missouri", "Columbia", "MO"), ("Springfield, Massachusetts", "Springfield", "MA"),
    ("Springfield, Illinois", "Springfield", "IL"), ("Rochester, Minnesota", "Rochester", "MN"),
    ("Kansas City, Kansas", "Kansas City", "KS"), ("Jackson, Tennessee", "Jackson", "TN"),
    ("Columbus, Georgia", "Columbus", "GA"), ("Arlington, Virginia", "Arlington", "VA"),
    ("Aurora, Illinois", "Aurora", "IL"), ("Richmond, California", "Richmond", "CA"),
    ("Uniondale", "Uniondale", "NY"), ("Athens, Georgia", "Athens", "GA"), ("Wilmington, North Carolina", "Wilmington", "NC"),
    ("Greenville, North Carolina", "Greenville", "NC"), ("Lafayette, Indiana", "Lafayette", "IN"),
    ("Bloomington, Illinois", "Bloomington", "IL"), ("Albany, Georgia", "Albany", "GA"),
]
for key, name, state in QUALIFIED:
    found = [c for c in by_name_country.get((name, "US"), []) if c["admin1"] == state]
    if not found:
        problems.append(f"qualified not found: {key}")
        continue
    cities[key] = (float(found[0]["lat"]), float(found[0]["lng"]))

aliases = {}
for alias, target in ALIASES.items():
    if alias == target:
        continue
    if target not in cities:
        problems.append(f"alias target missing: {alias} -> {target}")
        continue
    if alias in cities:
        continue
    aliases[alias] = target


def fmt(x):
    return f"{x:.3f}".rstrip("0").rstrip(".")


lines = []
lines.append("// GENERATED by tools/cityLocations/build.py - the cities a team can be placed")
lines.append("// in for travel: [latitude, longitude]. US cities of about 90,000 people or")
lines.append("// more (plotly's 2014 US cities), smaller ones with a team history, and the")
lines.append("// world's big cities, snapped to GeoNames (CC BY 4.0).")
lines.append("export const CITY_LOCATIONS: Record<string, [number, number]> = {")
for name in sorted(cities):
    lat, lon = cities[name]
    lines.append(f"\t{json.dumps(name)}: [{fmt(lat)}, {fmt(lon)}],")
lines.append("};")
lines.append("")
lines.append("// Other names for a city in CITY_LOCATIONS: states, nicknames, old names.")
lines.append("export const CITY_ALIASES: Record<string, string> = {")
for alias in sorted(aliases):
    lines.append(f"\t{json.dumps(alias)}: {json.dumps(aliases[alias])},")
lines.append("};")
lines.append("")
open(out_path, "w").write("\n".join(lines))
print(len(cities), "cities,", len(aliases), "aliases")
print("\n".join(problems))
