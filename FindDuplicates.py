# This class parses and cleans addresses of assets and deduplicates them based on address and name
# instantiating the class and then doing the writeToFile() function will call all of the methods in the class
# a CSV file with the merged duplicates is outputted as well as a CSV file of assets that were missing location information
# (such as PO Boxes) and a CSV file for any assets that threw an error while their address was  being parsed 

# the optional function at the end (location_check) checks that any asset with an updated location ID did not have its location information drastically changed 

class FindDuplicates:
    fileName = ""
    assetsFile = ""
    global df
    global assetdf
    assetdf = pd.DataFrame()
    # df is the data frame created from the raw csv file that is passed into the class to instantiate it 
    df = pd.DataFrame()
    # parseErrors is a dataframe that holds assets with addresses that couldnt be parsed correctly/ are missing either a street   number, street name, and/or zip code
    global parseErrors
    parseErrors = pd.DataFrame()
    # outputfile is the data frame that gets exported as the merge-instructions csv at the end of the code
    global missingValues
    missingValues = pd.DataFrame()
    global outputfile
    global count
    count = 0

    
    def __init__(self, fileName, assetsFile):
        self.fileName = fileName
        self.assetsFile = assetsFile
        self.assetdf = pd.read_csv(assetsFile)
        self.df = pd.read_csv(fileName)
        self.df['order'] = 0
        #the reason I did this instead of just calling the index of an asset is that
        #for some reason when I did row.index it did not just return a number
        #it returned a bunch of information about the asset
        #so I decided to set an "order" column that I could easily get as row.order later in the code
        for index, row in self.df.iterrows():
            idNum = row.id
            self.df.loc[self.df['id'] == idNum, 'order'] = index

        print("---")

        
    # parseAddresses iterates through the data frame and concatenates the street_address, city, state, and zip_code into a full address for each asset/row 
    # then uses usaddress.tag to parse the full address
    #     if an error is thrown because of address parsing its handled- the row with the address that caused the error is put 
    #     into a separate dataframe (parseError data frame) that holds all the rows that caused address parsing errors. at the end of this code 
    #     this data frame is exported to its own csv file 
    # the method then writes the parsed address into separate columns (like StreetNumber, StreetName) for each asset
    def parseAddresses(self, check=None):

        def isNaN(string):
            return string != string

        orderedColumns = [u'Unnamed: 0', u'Unnamed: 0.1', u'order', u'flags', u'id', u'name', u'AddressNumber',
                          u'StreetNamePreDirectional', u'StreetName', u'StreetNamePostType', u'location_id',
                          u'primary', u'ids_to_merge', u'asset_type', u'asset_id', u'tags',
                          u'street_address', u'city', u'state', u'zip_code', u'latitude',
                          u'longitude', u'SecondStreetName', u'SecondStreetNamePostType', u'StreetNamePostModifier',
                          u'SecondStreetNamePostDirectional',
                          u'parcel_id', u'residence', u'available_transportation',
                          u'parent_location', u'url', u'email', u'phone', u'hours_of_operation',
                          u'holiday_hours_of_operation', u'periodicity', u'capacity',
                          u'wifi_network', u'internet_access', u'computers_available',
                          u'open_to_public', u'child_friendly', u'sensitive', u'do_not_display',
                          u'localizability', u'services', u'accessibility',
                          u'hard_to_count_population', u'data_source_name', u'data_source_url',
                          u'organization_name', u'organization_phone', u'organization_email',
                          u'etl_notes', u'primary_key_from_rocket', u'synthesized_key',
                          u'geocoding_properties', u'AddressNumberPrefix', u'AddressNumberSuffix',
                          u'StreetNamePreModifier', u'StreetNamePreType',
                          u'StreetNamePostDirectional', u'SubaddressType',
                          u'SubaddressIdentifier', u'BuildingName', u'OccupancyType',
                          u'OccupancyIdentifier', u'CornerOf', u'LandmarkName', u'PlaceName',
                          u'StateName', u'ZipCode', u'USPSBoxType', u'USPSBoxID',
                          u'USPSBoxGroupType', u'USPSBoxGroupID', u'IntersectionSeparator',
                          u'Recipient', u'NotAddress', u'check']
        self.df = pd.DataFrame(data=self.df, columns=orderedColumns)
        # parseErrors data frame holds assets that throw an error when being parsed
        parseErrors = pd.DataFrame(data=None, columns=self.df.columns)
        self.parseErrors = parseErrors

        missingValues = pd.DataFrame(data=None, columns=self.df.columns)
        self.missingValues = missingValues

        numIter = len(self.df)
        if not check == None:
            numIter = check

        for index, row in self.df.iterrows():
            if not check == None:
                if numIter == 0:
                    break
            a = np.NaN
            idnum = row.id
            fulladdress = []
            tag = False
            parse = False
            exception = False
            numIter -= 1

            if not isNaN(row.street_address):
                streetaddress = row.street_address
                streetaddress = str(streetaddress)
                fulladdress.append(streetaddress)
            if not isNaN(row.city):
                city = row.city
                city = str(city)
                fulladdress.append(city)
            if not isNaN(row.state):
                state = row.state
                state = str(state)
                fulladdress.append(state)
            if not isNaN(row.zip_code):
                zip = row.zip_code
                zip = str(zip)
                z = zip.split('-')
                zip = z[0]
                fulladdress.append(zip)
            fulladdress = " ".join(fulladdress)
            try:
                c, d = usaddress.tag(fulladdress)
                tag = True
                a = c.items()
                values = []
                types = []
            except:
                try:
                    a = ''
                    values = []
                    types = []
                    a = usaddress.parse(fulladdress)
                    parse = True
                except:
                    err_df = self.df.loc[(self.df['id'] == idnum)]
                    self.parseErrors = self.parseErrors.append(err_df, sort=True)
                    exception = True
                    pass
            if exception == False:
                if tag == True:
                    for index, row in a.__iter__():
                        # types[] holds the type of parsed component, ex: StreetName
                        types.append(index)
                        # values [] holds the parsed address items ex: Forbes 
                        values.append(row)
                if parse == True:
                    for index, row in a.__iter__():
                        types.append(row)
                        values.append(index)
                i = 0
                for index, row in a.__iter__():
                    values[i] = values[i].lower()
                    self.df.loc[self.df['id'] == idnum, [types[i]]] = values[i]
                    i += 1

        self.df = self.df

        print("parsed addresses completed")
        self.parseErrors = self.parseErrors
        return self.df, self.parseErrors

    # cleanAddresses goes through the data frame, which now has parsed addresses, and standardizes the parsed addresses
    # the columns that this method standardizes are only used to identify duplicates, the original address values aren't changed
    def cleanAddresses(self):
        def isNaN(string):
            return string != string

        self.df.replace(regex=[' +'], value=' ', inplace=True)
        self.df['StreetNamePreDirectional'].replace(regex=['^e$|^e.$'], value='east', inplace=True)
        self.df['StreetNamePreDirectional'].replace(regex=['^n$|^n.$'], value='north', inplace=True)
        self.df['StreetNamePreDirectional'].replace(regex=['^s$|^s.$'], value='south', inplace=True)
        self.df['StreetNamePreDirectional'].replace(regex=['^w$|^w.$'], value='west', inplace=True)

        self.df['StreetNamePostType'].replace(regex=['highway$|hwy.$'], value="hwy", inplace=True)
        self.df['StreetNamePostType'].replace(regex=['boulevard$|blvd.$'], value="blvd", inplace=True)
        self.df['StreetNamePostType'].replace(regex=['drive$|dr.$'], value="dr", inplace=True)
        self.df['StreetNamePostType'].replace(regex=['road$|rd.$'], value="rd", inplace=True)
        self.df['StreetNamePostType'].replace(regex=['exit$|ext.$'], value="ext", inplace=True)
        self.df['StreetNamePostType'].replace(regex=['square$|sq.$'], value="sq", inplace=True)
        self.df['StreetNamePostType'].replace(regex=['circle$|cir.$'], value="cir", inplace=True)
        self.df['StreetNamePostType'].replace(regex=['parkway$|pkwy.$'], value="pkwy", inplace=True)
        self.df['StreetNamePostType'].replace(regex=['point$|pt.$'], value="pt", inplace=True)
        self.df['StreetNamePostType'].replace(regex=['pike$|pke.$'], value="pke", inplace=True)
        self.df['StreetNamePostType'].replace(regex=['pk$|pk.$'], value="park", inplace=True)
        self.df['StreetNamePostType'].replace(regex=['street$|st.$'], value='st', inplace=True)
        self.df['StreetNamePostType'].replace(regex=['avenue$|ave.$'], value="ave", inplace=True)
        self.df['StreetNamePostType'].replace(regex=['lane$|ln.$'], value='ln', inplace=True)
        self.df['StreetNamePostType'].replace(regex=['hills$|hls.$'], value="hls", inplace=True)
        self.df['StreetNamePostType'].replace(regex=['crossing$|xing.$'], value="xing", inplace=True)
        self.df['StreetNamePostType'].replace(regex=['expressway$|expy.$'], value="expy", inplace=True)
        self.df['StreetNamePostType'].replace(regex=['freeway$|frwy$|frwy.$|fwy$|fwy.'], value="fwy", inplace=True)
        self.df['StreetNamePostType'].replace(regex=['place$|pl.$'], value='pl', inplace=True)
        self.df['StreetNamePostType'].replace(regex=['route$|rte.$'], value="rte", inplace=True)
        self.df['StreetNamePostType'].replace(
            regex=['turnpike$|tpk.$|tpke$|tpke.$|trnpk$|trnpk.$|trpk$|trpk.$|turnpk$'], value='tpk', inplace=True)

        self.df['StreetNamePostDirectional'].replace(regex=['^e$|^e.$'], value='east', inplace=True)
        self.df['StreetNamePostDirectional'].replace(regex=['^n$|^n.$'], value='north', inplace=True)
        self.df['StreetNamePostDirectional'].replace(regex=['^s$|^s.$'], value='south', inplace=True)
        self.df['StreetNamePostDirectional'].replace(regex=['^w$|^w.$'], value='west', inplace=True)

        self.df['StreetName'].replace(regex=['^first'], value='1st', inplace=True)
        self.df['StreetName'].replace(regex=['^second'], value='2nd', inplace=True)
        self.df['StreetName'].replace(regex=['^third'], value='3rd', inplace=True)
        self.df['StreetName'].replace(regex=['^fourth'], value='4th', inplace=True)
        self.df['StreetName'].replace(regex=['^fifth'], value='5th', inplace=True)
        self.df['StreetName'].replace(regex=['^sixth'], value='6th', inplace=True)
        self.df['StreetName'].replace(regex=['^seventh'], value='7th', inplace=True)
        self.df['StreetName'].replace(regex=['^eight'], value='8th', inplace=True)
        self.df['StreetName'].replace(regex=['^ninth'], value='9th', inplace=True)
        self.df['StreetName'].replace(regex=['^tenth'], value='10th', inplace=True)
        self.df['StreetName'].replace(regex=['^eleventh'], value='11th', inplace=True)
        self.df['StreetName'].replace(regex=['^twelfth'], value='12th', inplace=True)
        self.df['StreetName'].replace(regex=['^thirteenth'], value='13th', inplace=True)
        self.df['StreetName'].replace(regex=['^fourteenth'], value='14th', inplace=True)
        self.df['StreetName'].replace(regex=['^fifteenth'], value='15th', inplace=True)
        self.df['StreetName'].replace(regex=['^twentieth'], value='20th', inplace=True)
        self.df['StreetName'].replace(regex=['twenty first|twenty-first'], value='21st', inplace=True)
        self.df['StreetName'].replace(regex=['twenty second|twenty-second'], value='22nd', inplace=True)
        self.df['StreetName'].replace(regex=['twenty third|twenty-third'], value='23rd', inplace=True)
        self.df['StreetName'].replace(regex=['twenty fourth|twenty-fourth'], value='24th', inplace=True)
        self.df['StreetName'].replace(regex=['twenty fifth|twenty-fifth'], value='25th', inplace=True)
        self.df['StreetName'].replace(regex=['twenty sixth|twenty-sixth'], value='26th', inplace=True)
        self.df['StreetName'].replace(regex=['twenty seventh|twenty-seventh'], value='27th', inplace=True)
        self.df['StreetName'].replace(regex=['twenty eight|twenty-eight'], value='28th', inplace=True)
        self.df['StreetName'].replace(regex=['twenty ninth|twenty-ninth'], value='29th', inplace=True)
        self.df['StreetName'].replace(regex=['thirtieth'], value='30th', inplace=True)
        self.df['StreetName'].replace(regex=['thirty first|thirty-first'], value='31st', inplace=True)
        self.df['StreetName'].replace(regex=['thirty second|thirty-second'], value='32nd', inplace=True)
        self.df['StreetName'].replace(regex=['thirty third|thirty-third'], value='33rd', inplace=True)
        self.df['StreetName'].replace(regex=['thirty fourth|thirty-fourth'], value='34th', inplace=True)
        self.df['StreetName'].replace(regex=['thirty fifth|thirty-fifth'], value='35th', inplace=True)
        self.df['StreetName'].replace(regex=['thirty sixth|thirty-sixth'], value='36th', inplace=True)
        self.df['StreetName'].replace(regex=['thirty seventh|thirty-seventh'], value='37th', inplace=True)
        self.df['StreetName'].replace(regex=['thirty eighth|thirty-eighth'], value='38th', inplace=True)
        self.df['StreetName'].replace(regex=['thirty ninth|thirty-ninth'], value='39th', inplace=True)
        self.df['StreetName'].replace(regex=['fortieth'], value='40th', inplace=True)
        self.df['StreetName'].replace(regex=['forty first|forty-first'], value='41st', inplace=True)
        self.df['StreetName'].replace(regex=['forty second|forty-second'], value='42nd', inplace=True)
        self.df['StreetName'].replace(regex=['forty third|forty-third'], value='43rd', inplace=True)
        self.df['StreetName'].replace(regex=['forty fourth|forty-fourth'], value='44th', inplace=True)
        self.df['StreetName'].replace(regex=['forty fifth|forty-fifth'], value='45th', inplace=True)
        self.df['StreetName'].replace(regex=['forty sixth|forty-sixth'], value='46th', inplace=True)
        self.df['StreetName'].replace(regex=['forty seventh|forty-seventh'], value='47th', inplace=True)
        self.df['StreetName'].replace(regex=['forty eighth|forty-eigth'], value='48th', inplace=True)
        self.df['StreetName'].replace(regex=['forty ninth|forty-ninth'], value='49th', inplace=True)
        self.df['StreetName'].replace(regex=['fiftieth'], value='50th', inplace=True)
        self.df['StreetName'].replace(regex=['fifty first|fifty-first'], value='51st', inplace=True)
        self.df['StreetName'].replace(regex=['fifty second|fifty-second'], value='52nd', inplace=True)
        self.df['StreetName'].replace(regex=['fifty third|fifty-third'], value='53rd', inplace=True)
        self.df['StreetName'].replace(regex=['fifty fourth|fifty-fourth'], value='54th', inplace=True)
        self.df['StreetName'].replace(regex=['fifty fifth|fifty-fifth'], value='55th', inplace=True)
        self.df['StreetName'].replace(regex=['fifty sixth|fifty-sixth'], value='56th', inplace=True)
        self.df['StreetName'].replace(regex=['fifty seventh|fifty-seventh'], value='57th', inplace=True)

        self.df = self.df
        print("clean addresses completed")

        return self.df

    #  mergeDups iterates through the main data frame and with each row, creates a new data frame made up of the current asset and duplicate entries of the current asset.
    # if there are >1 entries in the new data frame(there's a duplicate), the names of the entries are compared and
    # if they are similar enough to be duplicates of the same asset( fuzzy ratio > =83) they are processed to be merged
    # if the fuzzy ratio is 70< ratio <83, the asset is flagged as a potential duplicate but not processed to be merged
    # if the fuzzy ratio is <70 set of duplicates is flagged because it shares an address but not processed to be merged
    # When processed, each column of the duplicate entries is checked for conflicting values (ex: they have different phone numbers)
    # if there is a conflicting value the assets are printed out for the user and the user can either choose which one should be considered the primary value and 
    # choose that to overwrite the other, or can just be flagged to be reviewed later
    def mergeDups(self, check=None):
        parseErrors = pd.DataFrame(data=None, columns=self.df.columns)
        self.parseErrors = parseErrors
        counter = 0
        outputfile = pd.DataFrame()
        numIter = len(self.df)
        if not check == None:
            numIter = check

        # method checks if value passed into it is NaN
        def isNaN(string):
            return string != string

        # for each iteration through the main data frame (self.df), dupdf is a new data frame that pulls in the current asset 
        # from df and any other assets that has the same address number, street name, and zip code as the current asset
        for index, row in self.df.iterrows():
            if numIter == 0:
                break
            dupdf = pd.DataFrame
            addressNum = addressPreDirectional = addressStreet = addressPostType = addressPostDirectional = ""
            addressPlaceName = addressStateName = addressZip = ""
            idNum = row.id
            numOfRows = 0
            thisIndex = index
            hasAddress = False
            mergeTypes = False
            if check != None:
                numIter -= 1

            # I noticed an error that happened a few times when parsing addresses,  
            # if the address was 1 Maple St, Pittsburgh PA 
            # it would parse the address as PlaceName= Maple, StateName=St Pittsburgh
            # this attempts to correct this error
            if row.StateName == 'St Pittsburgh':
                print("St Pittsburgh error!!!")
                tempStreetName = row.PlaceName
                n = row.StateName.split()
                s = n[0]
                tempStreetName += " "
                tempStreetName += s
                print(tempStreetName)
                row.StreetName = tempStreetName
                row.City = 'Pittsburgh'

            # checks that there is an address number, street name, and zip code for the asset
            if not isNaN(row.AddressNumber) and not isNaN(row.StreetName) and not isNaN(row.ZipCode):
                addressNum = row.AddressNumber
                addressStreet = row.StreetName
                addressZip = row.ZipCode
                hasAddress = True

            # if the asset doesn't have an address num, street name, and zip code its added to a data frame called missingValues
            # the missingValues data frame is exported to its own csv file at the end of the code
            if hasAddress == False:
                err_df = self.df.loc[(self.df['id'] == idNum)]
                self.missingValues = self.missingValues.append(err_df, sort=True)
                self.missingValues = self.missingValues

            if hasAddress == True:
                # this is where the duplicates of the current asset & the current asset itself are put into dup df
                dupdf = pd.DataFrame(self.df.loc[(self.df['AddressNumber'] == addressNum) & (
                        self.df['StreetName'] == addressStreet) & (self.df['ZipCode'] == addressZip)])
                numOfRows = dupdf.shape[0]
                i = numOfRows
                rowOrder = row.order

                # this method accepts a list and returns the number of non duplicate items in the list
                def numItems(list):
                    res = []
                    [res.append(x) for x in list if x not in res]
                    n = 0
                    for i in res:
                        n += 1
                    return n

                # this method accepts a data frame and adds a flag and asks the user for a note to add in the flag column
                # and returns a data frame with this new flag added
                def addFlag(dupdf):
                    f = " ** "
                    notesInput = raw_input("flag notes: ")
                    notesInput = str(notesInput)
                    f += notesInput
                    existingFlag = ""
                    for index, row in dupdf.iterrows():
                        s = dupdf.at[index, 'flags']
                        s = str(s)
                        newFlag = s + f
                        dupdf.at[index, 'flags'] = newFlag
                    return dupdf
                
                #code from asset updater to find distance between geocoordinates
                
                def distance_on_unit_sphere(lat1, long1, lat2, long2):
                    # Convert latitude and longitude to
                    # spherical coordinates in radians.
                    degrees_to_radians = math.pi / 180.0

                    # phi = 90 - latitude
                    phi1 = (90.0 - lat1) * degrees_to_radians
                    phi2 = (90.0 - lat2) * degrees_to_radians

                    # theta = longitude
                    theta1 = long1 * degrees_to_radians
                    theta2 = long2 * degrees_to_radians

                    # Compute spherical distance from spherical coordinates.

                    # For two locations in spherical coordinates
                    # (1, theta, phi) and (1, theta', phi')
                    # cosine( arc length ) =
                    # sin phi sin phi' cos(theta-theta') + cos phi cos phi'
                    # distance = rho * arc length

                    cos = (math.sin(phi1) * math.sin(phi2) * math.cos(theta1 - theta2) +
                           math.cos(phi1) * math.cos(phi2))

                    try:
                        arc = math.acos(cos)
                    except ValueError:
                        if 1.0 < cos < 1.01:
                            cos = 1
                        try:
                            arc = math.acos(cos)
                        except ValueError:
                            ic(cos)
                            raise
                    # Remember to multiply arc by the radius of the earth
                    # in your favorite set of units to get length.
                    return arc

                def distance(lat1, long1, lat2, long2):
                    if lat1 in ['', None] or long1 in ['', None] or lat2 in ['', None] or long2 in ['', None]:
                        return None  # Don't try to calculate distances of invalid coordinates.
                    arc = distance_on_unit_sphere(lat1, long1, lat2, long2)
                    R = 20.902 * 1000 * 1000  # in feet
                    # Remember to multiply arc by the radius of the earth
                    # in your favorite set of units to get length.
                    return R * arc

                
                
                # to avoid finding the same set of duplicates twice or more times, the 
                # order numbers(set at the beginning of the code when class is instantiated) of 
                # the duplicates are compared and if any are less than the current index number 
                # (meaning they have already been iterated over and processed)
                # the set of duplicates is skipped
                if numOfRows > 1:
                    for index, row in dupdf.iterrows():
                        if row.order < rowOrder:
                            i = i - 1

                if i == numOfRows:
                    # 'primary' ,'ids_to_merge', and 'flags' columns are added to the dupdf
                    numOfRows = dupdf.shape[0]
                    dupdf['primary'] = 0
                    dupdf['ids_to_merge'] = ""
                    dupdf['flags'] = ""
                    ratio = 100
                    listofids = []  # type:List[str]
                    listofTags = []  # type: List[str]
                    phoneList = []  # type: List[int]
                    emailList = []  # type: List[str]
                    hoursOpList = []  # type: List[str]
                    capacityList = []  # type: List[int]
                    parentlocList = []  # type: List[str]
                    resList = []  # type: List[bool]
                    compList = []  # type: List[bool]
                    displayList = []  # type: List[bool]
                    cfList = []  # type: List[bool]
                    urlList = []  # type: List[str]
                    notesList = []  # type: List[str]
                    sensList = []  # type: List[bool]
                    typeList = []  # type: List[str]
                    thisLat = dupdf.at[thisIndex, 'latitude']
                    thisLong = dupdf.at[thisIndex, 'longitude']
                    Str1 = dupdf.at[thisIndex, 'name']
                    Str1 = str(Str1)
                    differentAssetTypes = False

                    # if there's more than one asset in the dupdf, a fuzzy comparison of the names of the assets happens
                    if numOfRows > 1:
                        for index, row in dupdf.iterrows():
                            tempStr = (dupdf.at[index, 'name'])
                            tempStr = str(tempStr)
                            ratio = fuzz.ratio(Str1.lower(), tempStr.lower())
                            if (ratio < 83):
                                break

                    # if the names have a ratio of <70 similarity they aren't processed as duplicates
                    # if they have the same address and* geocoordinates they're flagged for review
                    if ratio < 70 and numOfRows > 1:
                        for index, row in dupdf.iterrows():
                            tempid = row.id
                            tempLat = row.latitude
                            tempLong = row.longitude
                            if tempLat != thisLat and tempLong != thisLong and tempid != idNum:
                                dupdf[dupdf.id != tempid]
                                b = len(dupdf)
                                if b == 1:
                                    break
                                else:
                                    continue
                            else:
                                for index, row in dupdf.iterrows():
                                    tempAssetID = row.asset_id
                                    tempdf = pd.DataFrame(self.assetdf.loc[(self.assetdf['id'] == tempAssetID)])
                                    dupdf.at[index, 'ids_to_merge'] = row.id
                                    if len(tempdf) > 0:
                                        sub_df = tempdf.iloc[0]
                                        loc_id = sub_df.get(key='location_id')
                                        dupdf.set_value(index, 'location_id', loc_id)

                                #   dupdf.at[index, 'location_id'] = loc_id
                                dupdf['flags'] = "!!!"
                                outputfile = outputfile.append(dupdf)
                                break

                    # if the fuzzy ratio is 70-83 the assets are flagged as potential duplicates
                    numOfRows = len(dupdf)
                    if 70 < ratio < 83 and len > 1:
                        for index, row in dupdf.iterrows():
                            dupdf.at[index, 'flags'] = "potential"
                            dupdf.at[index, 'ids_to_merge'] = row.id
                            tempAssetID = row.asset_id
                            tempdf = pd.DataFrame(self.assetdf.loc[(self.assetdf['id'] == tempAssetID)])
                            if len(tempdf) > 0:
                                sub_df = tempdf.iloc[0]
                                loc_id = sub_df.get(key='location_id')
                                dupdf.set_value(index, 'location_id', loc_id)

                        outputfile = outputfile.append(dupdf)

                    if ratio >= 83 and numOfRows > 1:
                        idNum = row.id
                        counter = 0

                        typeList = []
                        for index, row in dupdf.iterrows():
                            tempType = row.asset_type
                            tempType = str(tempType)
                            if not isNaN(tempType):
                                typeList.append(tempType)
                        num = 0
                        if not typeList == []:
                            num = numItems(typeList)
                        if num == 1:
                            mergeTypes = True
                        elif num > 1:
                            print
                            print("--------------------------------------------")
                            print("Conflicting asset types: ")
                            print(dupdf[['name', 'asset_type', 'street_address']])
                            print
                            userInput = ''
                            while True:
                                userInput = raw_input("Do you want to merge these asset types? Y or N")
                                try:
                                    x = isinstance(5, str)
                                except ValueError:
                                    print 'Y or N value only'
                                    continue
                                if userInput == 'Y' or userInput == 'N':
                                    break
                                else:
                                    print 'Please enter either Y or N'
                            userInput = str(userInput)
                            if userInput == 'Y':
                                mergeTypes = True
                                differentAssetTypes = True

                            else:
                                # if user inputs N, the assets are flagged as having different asset types, 
                                # not processed as duplicates and written to the output file
                                mergeTypes = False
                                dupdf['flags'] = "**asset types**"
                                for index, row in dupdf.iterrows():
                                    dupdf.at[index, 'ids_to_merge'] = row.id
                                    tempAssetID = row.asset_id
                                    tempdf = pd.DataFrame(self.assetdf.loc[(self.assetdf['id'] == tempAssetID)])
                                    if len(tempdf) > 0:
                                        sub_df = tempdf.iloc[0]
                                        loc_id = sub_df.get(key='location_id')
                                        dupdf.set_value(index, 'location_id', loc_id)
                                outputfile = outputfile.append(dupdf)

                    # if there's only one asset in the duplicate df it is the original asset and doesn't have any duplicates
                    # so its not put through the merging process and is added to the outputfile data frame here
                    # outputfile is the data frame that gets exported as the merge-instructions csv at the end of the code
                    if numOfRows == 1:
                        dupdf.at[index, 'ids_to_merge'] = row.id
                        dupdf.at[index, 'primary'] = 1
                        tempAssetID = row.asset_id
                        tempdf = pd.DataFrame(self.assetdf.loc[(self.assetdf['id'] == tempAssetID)])
                        if len(tempdf) > 0:
                            sub_df = tempdf.iloc[0]
                            loc_id = sub_df.get(key='location_id')
                            dupdf.at[index, 'location_id'] = loc_id

                        outputfile = outputfile.append(dupdf)

                        
                    # if theres more than 1 row left in the dupdf and their names have a fuzzy score >=83 they go 
                    # through the process of being merged
                    elif numOfRows > 1 and ratio >= 83 and mergeTypes == True:
                        print
                        print("--------------------------------------------")
                        print

                        # the current asset in the main df is marked as the "primary" entry and any column values
                        # that should be in the final merged version of the asset are written to the primary asset 
                        #        ex: the phone number chosen by the user as the correct one out of two conflicting 
                        #        phone numbers will be written 
                        #        to the primary entry
                        dupdf.loc[dupdf['id'] == idNum, ['primary']] = 1
                        if ratio != 100:
                            tempNames = dupdf[['name']].values
                            print("Different asset names: ")
                            print(dupdf[['name', 'street_address']])
                            print
                            userID = input("Row with primary name, or 0 to flag and skip: ")
                            if userID == 0:
                                dupdf = addFlag(dupdf)
                            else:
                                dupdf.loc[dupdf['primary'] == 1, ['name']] = tempNames[userID - 1]
                        # a list of ids to merge separated by + is made and added to the primary entry

                        idcounter = 1
                        for index, row in dupdf.iterrows():
                            tempID = row.id
                            tempID = str(tempID)
                            listofids.append(tempID)
                            if idcounter < numOfRows:
                                listofids.append('+')
                            idcounter += 1
                        string_listofIDs = "".join(listofids)
                        dupdf.loc[dupdf['primary'] == 1, ['ids_to_merge']] = string_listofIDs

                        
                        # the tags of the duplicate entries are joined, separated by ',' and added to the primary entry
                        for index, row in dupdf.iterrows():
                            tempTag = row.tags
                            tempTag = str(tempTag)
                            if not isNaN(tempTag):
                                if tempTag not in listofTags:
                                    listofTags.append(tempTag)

                        string_listofTags = ", ".join(listofTags)
                        if not (isNaN(string_listofTags)):
                            dupdf.loc[dupdf['primary'] == 1, ['tags']] = string_listofTags

                            
                        # the dupdf is iterated through and for each column
                        # each of the assets are checked for an existing value
                        # if there is a value in one asset but not the others, this value is added to the primary entry
                        # if there's multiple conflicting values for the same column 
                        # the user is asked to either choose one value to be written to the primary entry to flag it for review

                        # asset type if theyre different
                        if differentAssetTypes == True:
                            typeList = []
                            printdf = dupdf
                            for index, row in dupdf.iterrows():
                                x = row.id
                                tempAssetType = row.asset_type
                                if not isNaN(tempAssetType):
                                    typeList.append(tempAssetType)
                                elif isNaN(tempAssetType):
                                    printdf = dupdf[dupdf.id != x]
                            num = 0
                            if not typeList == []:
                                num = numItems(typeList)

                            if num == 1:
                                dupdf.loc[dupdf['primary'] == 1, ['asset_type']] = phoneList[0]
                            elif num > 1:
                                print
                                print("conflicting asset types: ")
                                print(printdf[['name', 'street_address', 'asset_type']])
                                print
                                print("Choose a row 1 - "),
                                print(len(printdf))
                                userID = input("Row with primary asset type, or 0 to flag and skip: ")

                                if userID == 0:
                                    dupdf = addFlag(dupdf)
                                else:
                                    dupdf.loc[dupdf['primary'] == 1, ['asset_type']] = typeList[userID - 1]
                                    
                                    
                        # phone column
                        x = ""
                        printdf = dupdf
                        for index, row in dupdf.iterrows():
                            x = row.id
                            tempPhone = row.phone
                            if not isNaN(tempPhone):
                                phoneList.append(tempPhone)
                            elif isNaN(tempPhone):
                                printdf = dupdf[dupdf.id != x]
                        num = 0
                        if not phoneList == []:
                            num = numItems(phoneList)

                        if num == 1:
                            dupdf.loc[dupdf['primary'] == 1, ['phone']] = phoneList[0]
                        elif num > 1:
                            print
                            print("conflicting phone numbers: ")
                            print(printdf[['name', 'street_address', 'phone']])
                            print
                            print("Choose a row 1 - "),
                            print(len(printdf))
                            userID = input("Row with primary phone, or 0 to flag and skip: ")

                            if userID == 0:
                                dupdf = addFlag(dupdf)
                            else:
                                dupdf.loc[dupdf['primary'] == 1, ['phone']] = phoneList[userID - 1]

                        # email column
                        x = ""
                        printdf = dupdf
                        for index, row in dupdf.iterrows():
                            tempEmail = row.email
                            if not (isNaN(tempEmail)):
                                emailList.append(tempEmail)
                        num = 0
                        if not emailList == []:
                            num = numItems(emailList)

                        if num == 1:
                            dupdf.loc[dupdf['primary'] == 1, ['email']] = emailList[0]
                        elif num > 1:
                            print
                            print("conflicting emails: ")
                            print(printdf[['name', 'street_address', 'email']])
                            print
                            print("Choose a row 1 - "),
                            print(len(printdf))
                            userID = input("Row with primary email, or 0 to flag and skip: ")
                            if userID == 0:
                                dupdf = addFlag(dupdf)
                            else:
                                dupdf.loc[dupdf['primary'] == 1, ['email']] = emailList[userID - 1]

                        # hours of operation column
                        x = ""
                        printdf = dupdf
                        for index, row in dupdf.iterrows():
                            tempHours = row.hours_of_operation
                            if not (isNaN(tempHours)):
                                hoursOpList.append(tempHours)
                        num = 0
                        if not hoursOpList == []:
                            num = numItems(hoursOpList)

                        if num == 1:
                            dupdf.loc[dupdf['primary'] == 1, ['hours_of_operation']] = hoursOpList[0]
                        elif num > 1:
                            print
                            print("conflicting hours of operation: ")
                            print(printdf[['name', 'street_address', 'hours_of_operation']])
                            print
                            print("Choose a row 1 - "),
                            print(len(printdf))
                            userID = input("Row with primary hours of operation, or 0 to flag and skip: ")
                            if userID == 0:
                                dupdf = addFlag(dupdf)
                            else:
                                dupdf.loc[dupdf['primary'] == 1, ['hours_of_operation']] = hoursOpList[userID - 1]

                        # capacity column
                        x = ""
                        printdf = dupdf
                        for index, row in dupdf.iterrows():
                            tempcap = row.capacity
                            if not (isNaN(tempcap)):
                                capacityList.append(tempcap)
                        num = 0
                        if not capacityList == []:
                            num = numItems(capacityList)

                        if num == 1:
                            dupdf.loc[dupdf['primary'] == 1, ['capacity']] = capacityList[0]
                        elif num > 1:
                            print
                            print("conflicting capacity: ")
                            print(printdf[['name', 'street_address', 'capacity']])
                            print
                            print("Choose a row 1 - "),
                            print(len(printdf))
                            userID = input("Row with primary capacity, or 0 to flag and skip: ")
                            if userID == 0:
                                dupdf = addFlag(dupdf)
                            else:
                                dupdf.loc[dupdf['primary'] == 1, ['capacity']] = capacityList[userID - 1]

                        # parent location column
                        x = ""
                        printdf = dupdf
                        for index, row in dupdf.iterrows():
                            tempPL = row.parent_location
                            if not (isNaN(tempPL)):
                                parentlocList.append(tempPL)

                        num = 0
                        if not parentlocList == []:
                            num = numItems(parentlocList)

                        if num == 1:
                            dupdf.loc[dupdf['primary'] == 1, ['parent_location']] = parentlocList[0]
                        elif num > 1:
                            print
                            print("conflicting parent locations: ")
                            print(printdf[['name', 'street_address', 'parent_location']])
                            print
                            print("Choose a row 1 - "),
                            print(len(printdf))
                            userID = input("Row with primary parent location, or 0 to skip and flag: ")
                            if userID == 0:
                                dupdf = addFlag(dupdf)
                            else:
                                dupdf.loc[dupdf['primary'] == 1, ['parent_location']] = parentlocList[userID - 1]

                        # residence column
                        x = ""
                        printdf = dupdf
                        for index, row in dupdf.iterrows():
                            tempRes = row.residence
                            if not (isNaN(tempRes)):
                                resList.append(tempRes)

                        num = 0
                        if not resList == []:
                            num = numItems(resList)

                        if num == 1:
                            dupdf.loc[dupdf['primary'] == 1, ['residence']] = resList[0]
                        elif num > 1:
                            print
                            print("conflicting residence: ")
                            print(printdf[['name', 'street_address', 'residence']])
                            print
                            print("Choose a row 1 - "),
                            print(len(printdf))
                            userID = input("Row with primary residence, or 0 to skip and flag: ")
                            if userID == 0:
                                dupdf = addFlag(dupdf)
                            else:
                                dupdf.loc[dupdf['primary'] == 1, ['residence']] = resList[userID - 1]

                        # computers available column
                        x = ""
                        printdf = dupdf
                        for index, row in dupdf.iterrows():
                            tempComp = row.computers_available
                            if not (isNaN(tempComp)):
                                compList.append(tempComp)

                        num = 0
                        if not compList == []:
                            num = numItems(compList)

                        if num == 1:
                            dupdf.loc[dupdf['primary'] == 1, ['computers_available']] = compList[0]
                        elif num > 1:
                            print
                            print("conflicting computers_available: ")
                            print(printdf[['name', 'street_address', 'computers_available']])
                            print
                            print("Choose a row 1 - "),
                            print(len(printdf))
                            userID = input("Row with primary computers_available, or 0 to skip and flag: ")
                            if userID == 0:
                                dupdf = addFlag(dupdf)
                            else:
                                dupdf.loc[dupdf['primary'] == 1, ['computers_available']] = compList[userID - 1]

                        # child friendly column
                        x = ""
                        printdf = dupdf
                        for index, row in dupdf.iterrows():
                            tempcf = row.child_friendly
                            if not (isNaN(tempcf)):
                                cfList.append(tempcf)

                        num = 0
                        if not cfList == []:
                            num = numItems(cfList)

                        if num == 1:
                            dupdf.loc[dupdf['primary'] == 1, ['child_friendly']] = cfList[0]
                        elif num > 1:
                            print
                            print("conflicting child_friendly: ")
                            print(printdf[['name', 'street_address', 'child_friendly']])
                            print
                            print("Choose a row 1 - "),
                            print(len(printdf))
                            userID = input("Row with primary child_friendly, or 0 to skip and flag: ")
                            if userID == 0:
                                dupdf = addFlag(dupdf)
                            else:
                                dupdf.loc[dupdf['primary'] == 1, ['child_friendly']] = cfList[userID - 1]

                        # do not display column
                        x = ""
                        printdf = dupdf
                        for index, row in dupdf.iterrows():
                            tempDisplay = row.do_not_display
                            if not (isNaN(tempDisplay)):
                                displayList.append(tempDisplay)

                        num = 0
                        if not displayList == []:
                            num = numItems(displayList)

                        if num == 1:
                            dupdf.loc[dupdf['primary'] == 1, ['do_not_display']] = displayList[0]
                        elif num > 1:
                            print
                            print("conflicting do_not_display: ")
                            print(printdf[['name', 'street_address', 'do_not_display']])
                            print
                            print("Choose a row 1 - "),
                            print(len(printdf))
                            userID = input("Row with primary do_not_display, or 0 to skip and flag: ")
                            if userID == 0:
                                dupdf = addFlag(dupdf)
                            else:
                                dupdf.loc[dupdf['primary'] == 1, ['do_not_display']] = displayList[userID - 1]

                        # url column
                        x = ""
                        printdf = dupdf
                        for index, row in dupdf.iterrows():
                            tempUrl = row.url
                            if not (isNaN(tempUrl)):
                                urlList.append(tempUrl)

                        num = 0
                        if not urlList == []:
                            num = numItems(urlList)

                        if num == 1:
                            dupdf.loc[dupdf['primary'] == 1, ['url']] = urlList[0]
                        elif num > 1:
                            print
                            print("conflicting urls: ")
                            print(printdf[['name', 'street_address', 'url']])
                            print
                            print("Choose a row 1 - "),
                            print(len(printdf))
                            userID = input("Row with primary url, or 0 to skip and flag: ")
                            if userID == 0:
                                dupdf = addFlag(dupdf)
                            else:
                                dupdf.loc[dupdf['primary'] == 1, ['url']] = urlList[userID - 1]

                                
                        # notes column
                        # notes have the option to be concatenated 
                        x = ""
                        printdf = dupdf
                        for index, row in dupdf.iterrows():
                            tempNotes = row.etl_notes
                            if not (isNaN(tempNotes)):
                                notesList.append(tempNotes)

                        num = 0
                        if not notesList == []:
                            num = numItems(notesList)

                        if num == 1:
                            dupdf.loc[dupdf['primary'] == 1, ['etl_notes']] = notesList[0]

                        elif num > 1:
                            print
                            print("conflicting notes: ")
                            print(printdf[['name', 'street_address', 'etl_notes']])
                            print
                            userChoice = input(
                                "Would you like one set of notes [1] or to concatenate all notes [2], or [0] to skip and flag: ")
                            if userChoice == 1:
                                print("Choose a row 1 - "),
                                print(len(printdf))
                                userID = input("Choose row with primary notes: ")
                                dupdf.loc[dupdf['primary'] == 1, ['etl_notes']] = notesList[userID - 1]
                            elif userChoice == 2:
                                notesString = ". ".join(notesList)
                                dupdf.loc[dupdf['primary'] == 1, ['etl_notes']] = notesString
                            elif userChoice == 0:
                                dupdf = addFlag(dupdf)

                                
                        #gets location information from location instance
                        def get_loc_info(location_id):
                            location_id = str(location_id)
                            location_id += '/'
                            URL_TEMPLATE = 'https://assets.wprdc.org/api/dev/assets/locations/'
                            # print(asset_id)
                            owner_name = ''
                            r = requests.get(URL_TEMPLATE + location_id);
                            street_address = ""
                            try:
                                if (r.ok):
                                    soup = BeautifulSoup(r.text, 'html.parser')
                                    newDictionary = json.loads(str(soup))
                                    properties = newDictionary['properties']
                                    full_address = properties['full_address']
                                    full_address = str(full_address)
                                    geo = newDictionary['properties']['geocoding_properties']
                                    print("full address: "),
                                    print(full_address)
                                    latitude = properties['latitude']
                                    latitude = str(latitude)
                                    longitude = properties['longitude']
                                    longitude = str(longitude)
                                    print("latitude: "),
                                    print(latitude),
                                    print(" longitude: "),
                                    print(longitude)
                                    print
                                    print("geocoding properties: "),
                                    print(geo)

                            finally:
                                return latitude, longitude

                            
                        locIDList = []
                        newdf = pd.DataFrame(data=None, columns=assetdf.columns)
                        tempdf = pd.DataFrame(data=None, columns=assetdf.columns)
                        for index, row in dupdf.iterrows():
                            tempAssetID = row.id
                            tempdf = pd.DataFrame(self.df.loc[(self.df['id'] == tempAssetID)])
                            tempLocID = row.location_id
                            #    for index, row in tempdf.iterrows():
                            #       tempLocID = row.location_id
                            if not isNaN(tempLocID):
                                locIDList.append(tempLocID)
                                newdf = newdf.append(tempdf)
                        if len(dupdf) == 1:
                            sub_df = tempdf.iloc[0]
                            loc_id = sub_df.iloc[0]['location_id']
                            dupdf.loc[dupdf['primary'] == 1, ['location_id']] = loc_id

                        num = 0
                        pd.options.display.width = None
                        if not locIDList == []:
                            num = numItems(locIDList)
                        if num == 1:
                            dupdf.loc[dupdf['primary'] == 1, ['location_id']] = locIDList[0]
                        elif num > 1:
                            print
                            print("Conflicting location IDs: ")
                            print
                            i = 0
                            distances = []
                            for index, row in newdf.iterrows():
                                print(i + 1),
                                print(") ")
                                tempName = (newdf.at[index, 'name'])
                                tempName = str(tempName)
                                tempLocation = (newdf.at[index, 'location_id'])
                                tempLocation = str(tempLocation)
                                print(tempName),
                                print
                                print("location_id: "),
                                print(tempLocation)
                                locationID = int(locIDList[i])
                                a = get_loc_info(locationID)
                                distances.append(a)
                                i += 1
                                print
                            i = 0
                            j = len(distances)
                            for x in distances:
                                if i == j - 1:
                                    break
                                else:
                                    a = distances[i]
                                    lat1 = a[0]
                                    lat1 = float(lat1)
                                    long1 = a[1]
                                    long1 = float(long1)
                                    b = distances[i + 1]
                                    lat2 = b[0]
                                    lat2 = float(lat2)
                                    long2 = b[1]
                                    long2 = float(long2)
                                    o = distance(lat1, long1, lat2, long2)
                                    print
                                    print("The distance between address "),
                                    print(i + 1),
                                    print(" and address "),
                                    print(i + 2),
                                    print(" = "),
                                    print(o),
                                    print(" ft")
                                    i += 1

                            print
                            print("Choose a row 1 - "),
                            print(len(newdf))
                            userID = np.NaN
                            userID = raw_input("Row with primary location_id, or 0 to skip and flag: ")
                            userID = int(userID)
                            if userID == 0:
                                dupdf = addFlag(dupdf)
                            else:
                                dupdf.loc[dupdf['primary'] == 1, ['location_id']] = locIDList[userID - 1]
                        outputfile = outputfile.append(dupdf)

        print("merge duplicates completed")
        return outputfile

    # writeToFile calls all of the methods in the FindDuplicates class to parse, clean, and merge the data.
    # the resulting data frame of processed assets is outputfile
    # which is written to a csv file that the user names (.csv ending is added by the code)
    # and the data frame with the assets that couldn't be parsed or were missing parts of their address
    # and weren't processed is parseErrors 
    # this is written to a different csv file that adds _parse_errors.csv to the end of the user's file name
    def writeToFile(self, check=None):
        print
        print("parsing addresses")
        print
        self.parseAddresses()
        print
        print("cleaning addresses")
        print
        self.cleanAddresses()
        print
        print("merging duplicates")
        print
        if check == None:
            self.outputfile = self.mergeDups()
        else:
            check = int(check)
            self.outputfile = self.mergeDups(check)
        print
        print("writing to csv file")
        print
        orderedColumns = [u'Unnamed: 0', u'Unnamed: 0.1', u'order', u'flags', u'id', u'name', u'AddressNumber',
                          u'StreetNamePreDirectional', u'StreetName', u'StreetNamePostType',
                          u'primary', u'ids_to_merge', u'asset_type', u'asset_id', u'location_id', u'tags',
                          u'street_address', u'city', u'state', u'zip_code', u'latitude',
                          u'longitude', u'SecondStreetName', u'SecondStreetNamePostType', u'StreetNamePostModifier',
                          u'SecondStreetNamePostDirectional',
                          u'parcel_id', u'residence', u'available_transportation',
                          u'parent_location', u'url', u'email', u'phone', u'hours_of_operation',
                          u'holiday_hours_of_operation', u'periodicity', u'capacity',
                          u'wifi_network', u'internet_access', u'computers_available',
                          u'open_to_public', u'child_friendly', u'sensitive', u'do_not_display',
                          u'localizability', u'services', u'accessibility',
                          u'hard_to_count_population', u'data_source_name', u'data_source_url',
                          u'organization_name', u'organization_phone', u'organization_email',
                          u'etl_notes', u'primary_key_from_rocket', u'synthesized_key',
                          u'geocoding_properties', u'AddressNumberPrefix', u'AddressNumberSuffix',
                          u'StreetNamePreModifier', u'StreetNamePreType',
                          u'StreetNamePostDirectional', u'SubaddressType',
                          u'SubaddressIdentifier', u'BuildingName', u'OccupancyType',
                          u'OccupancyIdentifier', u'CornerOf', u'LandmarkName', u'PlaceName',
                          u'StateName', u'ZipCode', u'USPSBoxType', u'USPSBoxID',
                          u'USPSBoxGroupType', u'USPSBoxGroupID', u'IntersectionSeparator',
                          u'Recipient', u'NotAddress']
        normalColumns = [u'id', u'flags', u'name', u'asset_type', u'asset_id', u'tags', u'street_address',
                         u'city', u'state', u'zip_code', u'latitude', u'longitude', u'parcel_id',
                         u'residence', u'available_transportation', u'parent_location', u'url',
                         u'email', u'phone', u'hours_of_operation',
                         u'holiday_hours_of_operation', u'periodicity', u'capacity',
                         u'wifi_network', u'internet_access', u'computers_available',
                         u'open_to_public', u'child_friendly', u'sensitive', u'do_not_display',
                         u'localizability', u'services', u'accessibility',
                         u'hard_to_count_population', u'data_source_name', u'data_source_url',
                         u'organization_name', u'organization_phone', u'organization_email',
                         u'etl_notes', u'primary_key_from_rocket', u'synthesized_key',
                         u'geocoding_properties']
        self.outputfile = pd.DataFrame(data=self.outputfile, columns=orderedColumns)
        #  self.outputfile.astype({'zip_code': 'int32'}).dtypes
        self.outputfile.astype({'accessibility': 'bool'}).dtypes
        self.parseErrors = pd.DataFrame(data=self.parseErrors, columns=normalColumns)
        self.missingValues = pd.DataFrame(data=self.missingValues, columns=normalColumns)
        print
        userFileName = raw_input("output file name: ")
        errorFileName = missingValuesName = userFileName
        userFileName += ".csv"
        errorFileName += "_parse_errors.csv"
        missingValuesName += "_missing_values.csv"
        errorFileName = str(errorFileName)
        userFileName = str(userFileName)
        missingValuesName = str(missingValuesName)
        print
        print("file names: ", userFileName, errorFileName, missingValuesName)
        self.outputfile.to_csv(userFileName)
        self.parseErrors.to_csv(errorFileName)
        self.missingValues.to_csv(missingValuesName)
        print
        print("write to file completed")

#checks that any asset with an updated location ID did not have its location information drastically changed 
    def location_check(self):
        # method checks if value passed into it is NaN
        def isNaN(string):
            return string != string

        finalCols = [u'id', u'flags', u'name', u'asset_type', u'asset_id', u'tags', u'RAW_STREET_ADDRESS',
                     u'FINALIZED_STREET_ADDRESS', u'RAW_CITY', u'FINALIZED_CITY', u'RAW_STATE', u'FINALIZED_STATE',
                     u'RAW_ZIP', u'FINALIZED_ZIP', u'RAW_LATITUDE', u'FINALIZED_LATITUDE', u'RAW_LONGITUDE',
                     u'FINALIZED_LONGITUDE', u'street_address',
                     u'city', u'state', u'zip_code', u'latitude', u'longitude', u'parcel_id',
                     u'residence', u'available_transportation', u'parent_location', u'url',
                     u'email', u'phone', u'hours_of_operation',
                     u'holiday_hours_of_operation', u'periodicity', u'capacity',
                     u'wifi_network', u'internet_access', u'computers_available',
                     u'open_to_public', u'child_friendly', u'sensitive', u'do_not_display',
                     u'localizability', u'services', u'accessibility',
                     u'hard_to_count_population', u'data_source_name', u'data_source_url',
                     u'organization_name', u'organization_phone', u'organization_email',
                     u'etl_notes', u'primary_key_from_rocket', u'synthesized_key',
                     u'geocoding_properties']
        newdf = pd.DataFrame(data=None, columns=assetdf.columns)
        tempdf = pd.DataFrame(data=None, columns=assetdf.columns)
        finaldf = pd.DataFrame(data=None, columns=finalCols)
        m = pd.DataFrame(data=None, columns=finalCols)

        for index, row in self.df.iterrows():
            tempStreetAddress = row.street_address
            tempZipCode = row.zip_code
            tempCity = row.city
            tempState = row.state
            tempLat = row.latitude
            tempLong = row.longitude
            tempAssetID = row.asset_id
            m = pd.DataFrame(self.df.loc[(self.df['id'] == row.id)])
            tempLocID = row.location_id
            numRows = 0
            if not isNaN(tempLocID):
                tempdf = pd.DataFrame(self.assetdf.loc[(self.assetdf['location_id'] == tempLocID)])
                numRows = len(tempdf)
            if isNaN(tempLocID):
                numRows = 0
            if numRows > 0:
                for index, row in m.iterrows():
                    sub_df = tempdf.iloc[0]
                    fin_street = sub_df.get(key='street_address')
                    fin_city = sub_df.get(key='city')
                    fin_state = sub_df.get(key='state')
                    fin_zip = sub_df.get(key='zip_code')
                    fin_lat = sub_df.get(key='latitude')
                    fin_long = sub_df.get(key='longitude')
                    m.set_value(index, 'FINALIZED_STREET_ADDRESS', fin_street)
                    m.set_value(index, 'RAW_STREET_ADDRESS', tempStreetAddress)
                    m.set_value(index, 'RAW_CITY', tempCity)
                    m.set_value(index, 'FINALIZED_CITY', fin_city)
                    m.set_value(index, 'RAW_STATE', tempState)
                    m.set_value(index, 'FINALIZED_STATE', fin_state)
                    m.set_value(index, 'RAW_ZIP', tempZipCode)
                    m.set_value(index, 'FINALIZED_ZIP', fin_zip)
                    m.set_value(index, 'RAW_LATITUDE', tempLat)
                    m.set_value(index, 'FINALIZED_LATITUDE', fin_lat)
                    m.set_value(index, 'RAW_LONGITUDE', tempLong)
                    m.set_value(index, 'FINALIZED_LONGITUDE', fin_long)
            finaldf = finaldf.append(m)

        finaldf = pd.DataFrame(data=finaldf, columns=finalCols)
        userFileName = raw_input("output file name for location_check() : ")
        userFileName = str(userFileName)
        userFileName += "_location_check.csv"
        finaldf.to_csv(userFileName)
        # print(finaldf) 
        print("exported file: "),
        print(userFileName)


        
      
