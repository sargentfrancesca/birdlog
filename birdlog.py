import sys
import sqlite3 as lite
import csv		

class BirdRFID:
	def __init__(self, uniqid, second, minute, hour, day, month, year, birdid, area, submitter):
		self.uniqid = uniqid
		self.second = second
		self.minute = minute
		self.hour = hour
		self.day = day
		self.month = month
		self.birdid = birdid
		self.area = area
		self.submitter = submitter

def dbConnect(query):
	try: 
		con = lite.connect('birds.db')

		with con:
			cur = con.cursor()
			cur.execute("CREATE TABLE IF NOT EXISTS Birds (uniqid TEXT, second TEXT, minute TEXT, hour TEXT, day TEXT, month TEXT, year TEXT, birdid TEXT, area TEXT, submitter TEXT)")
			cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uniqid_idx ON Birds (uniqid)")
			cur.execute(query)
			con.commit()

	except lite.Error, e:
    
	    if con:
	        con.rollback()
	        
	    print "Error %s:" % e.args[0]
	    sys.exit(1)
	    
	finally:
	    print "Adding row to main database birds.db..."
	    con.commit()
	    if con:
	        con.close() 


def importCSV():
	print "Importing file..."

	filename = "BIRDRFID.LOG"
	birdArray = []
	eachBird = []
	
	with open(filename) as f:
	    for line in f:
	        birdArray.append([str(n) for n in line.strip().split(' ')])
	    area = promptExtra()

	for row in enumerate(birdArray):
	    try:
			second = row[1][0]
			minute = row[1][1]
			hour = row[1][2]
			day = row[1][3]
			month= row[1][4]
			year = row[1][5]
			birdid = row[1][6]
			area = area
			i = str(row[0])
			uniqid = i+second+minute+hour+day+month+year+i

			print uniqid

			eachBird.append({'uniqid': uniqid, 'second' : second, 'minute' : minute, 'hour' : hour, 'day' : day, 'month' : month, 'year' : year, 'birdid' : birdid, 'area' : area[0], 'submitter' : area[1]})

	    
	    except IndexError:
	        print "A line in the file doesn't have enough entries."	

	for bird in eachBird:
		columns = ', '.join(bird.keys())
		placeholders = '"'+'", "'.join(bird.values())
		placeholders += '"'
		
		
		query = 'INSERT OR IGNORE INTO Birds (%s) VALUES (%s)' % (columns, placeholders)
		dbConnect(query)


	print "Added to birds.db"

	var = raw_input('Would you like to back this up into the master csv? (y/n): ')

	if var is 'y':
		print "Backing up to csv file bird-master.csv..."
		masterWrite()

	elif var is 'n':
		print "Not backed up to csv file bird-master.csv"

	else:
		print "Unknown input, file not backed up to csv file bird-master.csv"
		sys.exit()

			


def promptExtra():
	area = raw_input('Which area were these recorded?: ')
	submitter = raw_input('Who is submitting this data?: ')
	return area, submitter


def masterWrite():
	conn = lite.connect('birds.db')
	conn.text_factory = str ## my current (failed) attempt to resolve this
	cur = conn.cursor()
	data = cur.execute("SELECT * FROM Birds")

	with open('bird-master.csv', 'wb') as f:
		writer = csv.writer(f)
		writer.writerow(['Uniqe ID', 'Second', 'Minute', 'Hour', 'Day', 'Month', 'Year', 'Bird ID', 'Area', 'Submitter'])
		writer.writerows(data)

	print "Data added to bird-master.csv"


importCSV()
