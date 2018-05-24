import happybase

conn = happybase.Connection('10.0.0.96')
if conn:
    print "Connectin Established"

#conn.create_table('siva', {'cf': dict()})
table = conn.table("siva")
table.put('id', {'cf:name': 'siva'}) 
