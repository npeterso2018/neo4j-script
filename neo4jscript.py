from neo4j import GraphDatabase
import math

uri = 'neo4j+s://5da7689b.databases.neo4j.io'
user = 'neo4j'
password = '99GvswkkKi8tNAQaEse_fMpXu74JD6n8uiScenTaPlk'
file = 'connections.csv'
connections = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vQFVUYVqr1l4Ulmr3_U_lRUX-XsjqdmkqVO5IhUTaysBwyjIo-pMjUGG6Dm6SJzQlR8FQgKXqJyg6oE/pub?gid=0&single=true&output=csv'

#conversion factor between coordinates and feet
factor = 0.000000809848792*4

#will get the distance between two points in Neo4j format
def getDistance(a,b):
	dx = float(a['long']) - float(b['long'])
	dy = float(a['lat']) - float(b['lat'])

	return(((dx**2 + dy**2)**0.5)/factor)

#will get the angle of the direction between two points
def getAngle(a,b):
	dx = float(b['long']) - float(a['long'])
	dy = float(b['lat']) - float(a['lat'])

	if dx > 0 and dy == 0:
		return(90)

	elif dx < 0 and dy == 0:
		return(-90)

	elif dx == 0 and dy < 0:
		return(180)

	elif dx > 0 and dy < 0:
		return(180 + math.atan(dx/dy)*180/math.pi)

	elif dx < 0 and dy < 0:
		return(-180 + math.atan(dx/dy)*180/math.pi)

	return(math.atan(dx/dy)*180/math.pi)

#will get the direction (N,E,S,W,etc.) between two points
def getDirection(a,b):
	angle = getAngle(a,b)
	directions = {0:'N',45:'NW',90:'E',135:'SE',180:'S',-180:'S',-135:'SW',-90:'W',-45:'NW'}
	for i in range(-180,181,45):
		if(isWithin(angle,i,45)):
			return(directions[i])

#helper function
def isWithin(n,c,t):
	return(n > c-t/2 and n <= c+t/2)

#reads a csv file and creates the connections in neo4j
def connectFromFile(file):
	f = open(file,'r')
	for l in f:
		createConnections(l)

#above, but takes only one line
def createConnections(line):
	line = line.split(',')
	aWID = line[0]
	i = 1
	while i < len(line)-1:
		bWID = line[i]
		stairs = line[i+1]

		linkPaths(aWID,bWID,stairs)

		i += 2


class Neo4jscript:
	def __init__(self,uri,user,password):
		self.driver = GraphDatabase.driver(uri,auth=(user,password))

	def close(self):
		self.driver.close()


	def lookup(tx,WID):
		with tx.driver.session() as session:
			result = session.run("MATCH(a) WHERE a.WID='" + WID + "' RETURN a")
			return(result.data()[0]['a'])

	#creates a path between two points
	def linkPaths(tx,aWID,bWID,stairs):
		with tx.driver.session() as session:
			bite = "MATCH (a), (b) WHERE a.WID='" + aWID + "' and b.WID='" + bWID + "'"
			
			ab = session.run(bite + " RETURN a,b").data()[0]
			
			a = ab['a']
			b = ab['b']

			distance = getDistance(a,b)
			direction = getDirection(a,b)

			if stairs=='TRUE':
				cmd = bite + " CREATE (a)-[r:Stairs{distance:" + str(distance) + ", direction:'" + str(direction) + "', stairs:" + str(stairs) + "}]->(b)"

			else:
				cmd = bite + " CREATE (a)-[r:Path{distance:" + str(distance) + ", direction:'" + str(direction) + "', stairs:" + str(stairs) + "}]->(b)"

			return(session.run(cmd))

	def exit(self):
		self.driver.close()

	#helper function
	def resetAndUpdateDatabase(tx):
		with tx.driver.session() as session:
			session.run("MATCH(n) DETACH DELETE n")
			return(session.run("LOAD CSV FROM '" + connections + "' AS LINE CREATE (:Point {WID: LINE[0], lat: LINE[1], long: LINE[2], type: LINE[3], name: LINE[4], description: LINE[5], facts: LINE[6], stories: LINE[7]}) RETURN LINE"))

	#deletes database. probably a bad idea to use
	def resetDatabase(tx):
		with tx.driver.session() as session:
			return(session.run("MATCH(n) DETACH DELETE n"))

#runs the script
if __name__ == '__main__':
	session = Neo4jscript('neo4j+s://5da7689b.databases.neo4j.io','neo4j','99GvswkkKi8tNAQaEse_fMpXu74JD6n8uiScenTaPlk')
	print(session.resetAndUpdateDatabase())

	connections = open(file,"r")

	for line in connections:
		i = 1

		line = line.split(",")
		a = line[0]

		while i < len(line)-1:
			b = line[i]
			s = line[i+1]
			i += 2

			if(b):
				print(session.linkPaths(a,b,s.strip("\n")))

	#print(session.linkPaths("2","1",True))
	session.exit()
	