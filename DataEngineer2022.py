import mysql.connector
from geopy.geocoders import Nominatim

geocoder = Nominatim(user_agent="WildCodeSchool")

connection_params = {
    'host': "localhost",
    'user': "root",
    'password': "Automatique1",
    'database': "DataEngineer2022",
}

request = "select address_id, address, city, postal_code from address"

db = mysql.connector.connect(**connection_params)

mycursor = db.cursor()
        
mycursor.execute(request)
        
addresses_final={}

addresses = mycursor.fetchall()

for address in addresses:
    
    addresses_final[address[0]] = address[1]+" "+address[2]+" "+address[3]



######################

latitudes={}
longitudes={}

for  address_id, address in addresses_final.items():
    try:
        location = geocoder.geocode(address)
        if location is not None:
            
            latitudes[address_id] = location.latitude
            longitudes[address_id] = location.longitude
                
        else:
            latitudes[address_id] = "Not found"
            longitudes[address_id] = "Not found"
        
    except:
        pass


#####################################

request_ADD = "ALTER TABLE address ADD COLUMN (latitude VARCHAR(50), longitude VARCHAR(50))"

try:
    mycursor.execute(request_ADD)
    db.commit()
    
except:
    pass # handle and ignore any "duplicate column name" errors

########################################

request_UPDATE_lati = "UPDATE address SET latitude = %s WHERE address_id= %s"



for  address_id, latitude in latitudes.items():
    
    params_lati = [(latitude, address_id)]
    mycursor.executemany(request_UPDATE_lati, params_lati)
    db.commit()

########################

request_UPDATE_longi = "UPDATE address SET longitude = %s WHERE address_id= %s"

for  address_id, longitude in longitudes.items():
    
    params_longi = [(longitude, address_id)]
    mycursor.executemany(request_UPDATE_longi, params_longi)
    db.commit()


request_UPDATE = "UPDATE address SET latitude = %s , longitude = %s WHERE latitude is NULL OR longitude is NULL"
params = [("Not found","Not found")]
mycursor.executemany(request_UPDATE,params)
db.commit()

 
request_top_tenant = "SELECT c.first_name, c.last_name, p.amount, a.address, a.city, a.postal_code, a.latitude, a.longitude \
                        FROM address a \
                        INNER JOIN customer c \
                        ON a.address_id = c.address_id \
                        INNER JOIN payment p \
                        ON c.customer_id = p.customer_id \
                        WHERE  p.amount = (SELECT MAX(amount) FROM payment p)"

mycursor.execute(request_top_tenant)

top_tenant_data = mycursor.fetchall()

for row in top_tenant_data:
    
    print("---------------------")
    print("first_name : ", row[0])
    print("last_name  : ", row[1])
    print("amount     : ", row[2])
    print("address    : ", row[3])
    print("city       : ", row[4])
    print("postal_code: ", row[5])
    print("latitude   : ", row[6])
    print("longitude  : ", row[7])
    print("---------------------")
    
    
db.close()