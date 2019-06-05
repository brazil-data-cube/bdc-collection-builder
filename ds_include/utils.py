import os
import io
import sqlalchemy
import time
import datetime
import fnmatch
import glob
import logging
import requests
import json
import numpy

###################################################
def c2jyd(calendardate):
	dt = numpy.datetime64(calendardate)
	year = calendardate[:4]
	jday = (dt - numpy.datetime64(year+'-01-01')).astype(int) + 1
	juliandate = year+'03d'.format(jday)
	return (juliandate,int(year),jday)


###################################################
def do_query(sql,one=False):
	connection = 'mysql://{}:{}@{}/{}'.format(os.environ.get('DB_USER'),
											  os.environ.get('DB_PASS'),
											  os.environ.get('DB_HOST'),
											  os.environ.get('DB_NAME'))
	engine = sqlalchemy.create_engine(connection)
	result = engine.execute(sql)
	if one:
		row = result.fetchone()
		engine.dispose()
		return row
	else:
		result = result.fetchall()
		engine.dispose()
		logging.warning('do_query - '+sql+' - Rows: {}'.format(len(result)))
		return [dict(row) for row in result]

###################################################
def do_command(sql):
	connection = 'mysql://{}:{}@{}/{}'.format(os.environ.get('DB_USER'),
											  os.environ.get('DB_PASS'),
											  os.environ.get('DB_HOST'),
											  os.environ.get('DB_NAME'))
	engine = sqlalchemy.create_engine(connection)
	result = engine.execute(sql)
	logging.warning('do_command - '+sql)
	engine.dispose()
	return


###################################################
def do_insert(table,activity):
# Inserting data into Scene table
	params = ''
	values = ''
	for key,val in activity.items():
		if val is None: continue
		params += key+','
		if type(val) is str:
				values += "'{0}',".format(val)
		else:
				values += "{0},".format(val)
			
	sql = "INSERT INTO {0} ({1}) VALUES({2})".format(table,params[:-1],values[:-1])
	connection = 'mysql://{}:{}@{}/{}'.format(os.environ.get('DB_USER'),
											  os.environ.get('DB_PASS'),
											  os.environ.get('DB_HOST'),
											  os.environ.get('DB_NAME'))
	logging.warning('do_insert - '+sql)
	engine = sqlalchemy.create_engine(connection)
	engine.execute(sql)
	engine.dispose()
	return


###################################################
def do_update(table,activity):
# Inserting data into activities table
	connection = 'mysql://{}:{}@{}/{}'.format(os.environ.get('DB_USER'),
											  os.environ.get('DB_PASS'),
											  os.environ.get('DB_HOST'),
											  os.environ.get('DB_NAME'))
	engine = sqlalchemy.create_engine(connection)

	params = ''
	id = activity['id']
	for key,val in activity.items():
		if key == 'id' or val is None: continue
		params += key+'='
		if type(val) is str or isinstance(val, datetime.date):
				params += "'{0}',".format(val)
		else:
				params += "{0},".format(val)
	sql = "UPDATE {} SET {} WHERE id = {}".format(table,params[:-1],id)
	logging.warning('do_update - '+sql)
	engine.execute(sql)
	engine.dispose()
	return


###################################################
def do_upsert(table,activity,avoidlist=None,verbose=True):
# Inserting data into activities table
	connection = 'mysql://{}:{}@{}/{}'.format(os.environ.get('DB_USER'),
											  os.environ.get('DB_PASS'),
											  os.environ.get('DB_HOST'),
											  os.environ.get('DB_NAME'))
	engine = sqlalchemy.create_engine(connection)

	params = '1'
	for key,val in activity.items():
		if  val is None or (avoidlist is not None and key in avoidlist): continue
		params += ' AND '+key+'='
		if type(val) is str or isinstance(val, datetime.date):
				params += "'{0}'".format(val)
		else:
				params += "{0}".format(val)
			
	sql = "SELECT * FROM {} WHERE {} ".format(table,params)
	results = engine.execute(sql)
	results = results.fetchall()
	if verbose: logging.warning('do_upsert - '+sql+' - Rows: {}'.format(len(results)))
	if len(results) > 0:
		params = ''
		for key,val in activity.items():
			if key == 'id' or val is None: continue
			params += key+'='
			if type(val) is str or isinstance(val, datetime.date):
					params += "'{0}',".format(val)
			else:
					params += "{0},".format(val)
		for result in results:
			id = result['id']
			sql = "UPDATE {} SET {} WHERE id = {}".format(table,params[:-1],id)
			if verbose: logging.warning('do_upsert - '+sql)
			engine.execute(sql)
	else:
		params = ''
		values = ''
		for key,val in activity.items():
			if key == 'id' or val is None: continue
			params += key+','
			if type(val) is str or isinstance(val, datetime.date):
					values += "'{0}',".format(val)
			else:
					values += "{0},".format(val)
			
		sql = "INSERT INTO {0} ({1}) VALUES({2})".format(table,params[:-1],values[:-1])
		if verbose: logging.warning('do_upsert - '+sql)
		engine.execute(sql)
	engine.dispose()
	return


#################################
def decodePathRow(pathrow):
	parts = pathrow.split(';')
	dpathrow = []
	for part in parts:
		pieces = part.split(',')
		if len(pieces) == 1: return [pathrow]
		if len(pieces) != 2:
			app.logger.exception( 'Sintax Error (missing ,)')
			return []
		paths = pieces[0].split(':')
		rows  = pieces[1].split(':')
		if len(paths) > 1:
			p1 = int(paths[0])
			p2 = int(paths[1]) + 1
		else:
			p1 = int(paths[0])
			p2 = p1 + 1
		if len(rows) > 1:
			r1 = int(rows[0])
			r2 = int(rows[1]) + 1
		else:
			r1 = int(rows[0])
			r2 = r1 + 1
		for p in range(p1,p2):
			for r in range(r1,r2):
				if (p,r) not in dpathrow:
					tileid = '{0:03d}{1:03d}'.format(p,r)
					dpathrow.append(tileid)
	return dpathrow


################################
def daysInMonth(date):
	year = int(date.split('-')[0])
	month = int(date.split('-')[1])
	nday = day = int(date.split('-')[2])
	if month == 12:
		nmonth = 1
		nyear = year +1
	else:
		nmonth = month + 1
		nyear = year
	ndate = '{0:4d}-{1:02d}-{2:02d}'.format(nyear,nmonth,nday)
	td = numpy.datetime64(ndate) - numpy.datetime64(date)
	return td
	

################################
def decodePeriods(temporalschema,startdate,enddate,timestep):
	logging.warning('decodePeriods - {} {} {} {}'.format(temporalschema,startdate,enddate,timestep))
	requestedperiods = {}
	if startdate is None:
		return requestedperiods
	if isinstance(startdate, datetime.date):
		startdate = startdate.strftime('%Y-%m-%d')

	tdtimestep = datetime.timedelta(days=timestep)
	stepsperperiod = int(round(365./timestep))

	if enddate is None:
		enddate = datetime.datetime.now().strftime('%Y-%m-%d')
	if isinstance(enddate, datetime.date):
		enddate = enddate.strftime('%Y-%m-%d')

	if temporalschema is None:
		periodkey = startdate + '_' + startdate + '_' + enddate
		requestedperiod = []
		requestedperiod.append(periodkey)
		requestedperiods[startdate] = requestedperiod
		return requestedperiods

	if temporalschema == 'M':
		start_date = numpy.datetime64(startdate)
		end_date = numpy.datetime64(enddate) 
		requestedperiod = []
		while start_date <= end_date:
			next_date = start_date + daysInMonth(str(start_date))
			periodkey = str(start_date)[:10] + '_' + str(start_date)[:10] + '_' + str(next_date - numpy.timedelta64(1, 'D'))[:10]
			requestedperiod.append(periodkey)
			requestedperiods[startdate] = requestedperiod
			start_date = next_date
		return requestedperiods

# Find the exact startdate based on periods that start on yyyy-01-01
	firstyear = startdate.split('-')[0]
	start_date = datetime.datetime.strptime(startdate, '%Y-%m-%d')
	if temporalschema == 'A':
		dbase = datetime.datetime.strptime(firstyear+'-01-01', '%Y-%m-%d')
		while dbase < start_date:
			dbase += tdtimestep
		if dbase > start_date:
			dbase -= tdtimestep
		startdate = dbase.strftime('%Y-%m-%d')
		start_date = dbase

# Find the exact enddate based on periods that start on yyyy-01-01
	lastyear = enddate.split('-')[0]
	end_date = datetime.datetime.strptime(enddate, '%Y-%m-%d')
	if temporalschema == 'A':
		dbase = datetime.datetime.strptime(lastyear+'-12-31', '%Y-%m-%d')
		while dbase > end_date:
			dbase -= tdtimestep
		end_date = dbase
		if end_date == start_date:
			end_date += tdtimestep - datetime.timedelta(days=1)
		enddate = end_date.strftime('%Y-%m-%d')

# For annual periods
	if temporalschema == 'A':
		dbase = start_date
		yearold = dbase.year
		count = 0
		requestedperiod = []
		while dbase < end_date:
			if yearold != dbase.year:
				dbase = datetime.datetime(dbase.year,1,1)
			yearold = dbase.year
			dstart = dbase
			dend = dbase + tdtimestep - datetime.timedelta(days=1)
			dend = min(datetime.datetime(dbase.year,12,31),dend)
			basedate = dbase.strftime('%Y-%m-%d')
			startdate = dstart.strftime('%Y-%m-%d')
			enddate = dend.strftime('%Y-%m-%d')
			periodkey = basedate + '_' + startdate + '_' + enddate
			if count % stepsperperiod == 0:
				count = 0
				requestedperiod = []
				requestedperiods[basedate] = requestedperiod
			requestedperiod.append(periodkey)
			count += 1
			dbase += tdtimestep
		if len(requestedperiods) == 0 and count > 0:
			requestedperiods[basedate].append(requestedperiod)
	else:
		yeari = start_date.year
		yearf = end_date.year
		monthi = start_date.month
		monthf = end_date.month
		dayi = start_date.day
		dayf = end_date.day
		logging.warning('decodePeriods - {} {} {} {} {} {}'.format(yeari,yearf,monthi,monthf,dayi,dayf))
		for year in range(yeari,yearf+1):
			dbase = datetime.datetime(year,monthi,dayi)
			if monthi <= monthf:
				dbasen = datetime.datetime(year,monthf,dayf)
			else:
				dbasen = datetime.datetime(year+1,monthf,dayf)
			while dbase < dbasen:
				dstart = dbase
				dend = dbase + tdtimestep - datetime.timedelta(days=1)
				basedate = dbase.strftime('%Y-%m-%d')
				startdate = dstart.strftime('%Y-%m-%d')
				enddate = dend.strftime('%Y-%m-%d')
				periodkey = basedate + '_' + startdate + '_' + enddate
				requestedperiod = []
				requestedperiods[basedate] = requestedperiod
				requestedperiods[basedate].append(periodkey)
				dbase += tdtimestep
	return requestedperiods


###################################################
def shrink(lonmin,latmax,lonmax,latmin):
	clon = (lonmin+lonmax)/2
	clat = (latmin+latmax)/2
	dlon = lonmax-lonmin
	dlat = latmax-latmin
	slonmin = clon-dlon/4
	slatmax = clat+dlat/4
	slonmax = clon+dlon/4
	slatmin = clat-dlat/4
	return slonmin,slatmax,slonmax,slatmin
	

