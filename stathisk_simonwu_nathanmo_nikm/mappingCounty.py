def getMappingCounty():
	res = []

	def splitAssignTag(string, tag):
		for item in string.split('|'):
			res.append({'Town' : item.strip(), 'County' :tag})

	first = "Barnstable |\
	Bourne |\
	Brewster |\
	Chatham |\
	Dennis |\
	Eastham |\
	Falmouth |\
	Harwich |\
	Mashpee |\
	Orleans |\
	Provincetown |\
	Sandwich |\
	Truro |\
	Wellfleet |\
	Yarmouth"
	splitAssignTag(first, "Barnstable")


	second = "Acushnet |\
	Berkley |\
	Dartmouth |\
	Dighton |\
	Fairhaven |\
	FALL RIVER |\
	Freetown |\
	NEW BEDFORD |\
	Raynham |\
	Somerset |\
	Swansea |\
	TAUNTON |\
	Westport |\
	ATTLEBORO |\
	Easton |\
	Mansfield |\
	North Attleborough |\
	Norton |\
	Rehoboth |\
	Seekonk"
	splitAssignTag(second, "Bristol")

	third = "Aquinnah |\
	Chilmark |\
	Edgartown |\
	Gosnold |\
	Oak Bluffs |\
	Tisbury |\
	West Tisbury"
	splitAssignTag(third, "Dukes")

	fouth = "Nantucket"
	splitAssignTag(fouth, "Nantucket")

	fifth = "Bridgewater |\
	Carver |\
	Kingston |\
	Lakeville |\
	Marion |\
	Mattapoisett |\
	Middleborough |\
	Pembroke |\
	Plymouth |\
	Rochester |\
	Wareham |\
	East Bridgewater |\
	West Bridgewater |\
	Abington |\
	BROCKTON |\
	Duxbury |\
	East Bridgewater |\
	Halifax |\
	Hanover |\
	Hanson |\
	Hingham |\
	Hull |\
	Marshfield |\
	Norwell |\
	Plympton |\
	Rockland |\
	Scituate |\
	Whitman"
	splitAssignTag(fifth, "Plymouth")


	sixth = "Ashland |\
	Framingham |\
	Holliston |\
	Hopkinton |\
	Natick |\
	Sherborn |\
	Wayland |\
	Acton |\
	Arlington |\
	Ayer |\
	Bedford |\
	Billerica |\
	Belmont |\
	Boxborough |\
	Burlington |\
	Carlisle |\
	Chelmsford |\
	Concord |\
	Hudson |\
	Lexington |\
	Lincoln |\
	Littleton |\
	MARLBOROUGH |\
	Maynard |\
	NEWTON |\
	Shirley |\
	Stow |\
	Sudbury |\
	WALTHAM |\
	Watertown |\
	Weston |\
	Woburn |\
	Dracut |\
	Dunstable |\
	Groton |\
	LOWELL |\
	North Reading |\
	Pepperell |\
	Tewksbury |\
	Tyngsborough |\
	Westford |\
	Wilmington |\
	CAMBRIDGE |\
	EVERETT |\
	MALDEN |\
	MEDFORD |\
	MELROSE |\
	Reading |\
	SOMERVILLE |\
	Stoneham |\
	Wakefield |\
	Winchester |\
	Ashby |\
	Townsend"

	splitAssignTag(sixth, "Middlesex")

	seventh = "Avon |\
	Braintree |\
	Canton |\
	Dedham |\
	Dover |\
	Foxborough |\
	Franklin |\
	Medfield |\
	Medway |\
	Millis |\
	Milton |\
	Needham |\
	Norfolk |\
	Norwood |\
	Plainville |\
	Randolph |\
	Sharon |\
	Stoughton |\
	Walpole |\
	Wellesley |\
	Westwood |\
	Wrentham |\
	Brookline |\
	Wellesley |\
	Braintree |\
	Cohasset |\
	Holbrook |\
	QUINCY |\
	Weymouth |\
	Bellingham"
	splitAssignTag(seventh, "Norfolk")



	eighth = "BOSTON |\
	CHELSEA |\
	REVERE |\
	Winthrop"
	splitAssignTag(eighth, "Suffolk")

	ninth = "Harvard |\
	Northborough |\
	Southborough |\
	Westborough |\
	Ashburnham |\
	Athol |\
	Auburn |\
	Barre |\
	Berlin |\
	Blackstone |\
	Bolton |\
	Boylston |\
	Brookfield |\
	Charlton |\
	Clinton |\
	Douglas |\
	Dudley |\
	East Brookfield |\
	FITCHBURG |\
	GARDNER |\
	Grafton |\
	Hardwick |\
	Holden |\
	Hopedale |\
	Hubbardston |\
	Lancaster |\
	Leicester |\
	LEOMINSTER |\
	Lunenburg |\
	Mendon |\
	Milford |\
	Millbury |\
	Millville |\
	New Braintree |\
	North Brookfield |\
	Northborough |\
	Northbridge |\
	Oakham |\
	Oxford |\
	Paxton |\
	Petersham |\
	Phillipston |\
	Princeton |\
	Rutland |\
	Shrewsbury |\
	Southbridge |\
	Spencer |\
	Sterling |\
	Sturbridge |\
	Sutton |\
	Templeton |\
	Upton |\
	Uxbridge |\
	Warren |\
	Webster |\
	West Boylston |\
	West Brookfield |\
	Westminster |\
	Winchendon |\
	WORCESTER |\
	Royalston"
	splitAssignTag(ninth, "Worcester")

	tenth = "Amesbury |\
	Andover |\
	BEVERLY |\
	Boxford |\
	Danvers |\
	Essex |\
	Georgetown |\
	GLOUCESTER |\
	Groveland |\
	Hamilton |\
	HAVERHILL |\
	Ipswich |\
	LAWRENCE |\
	Manchester |\
	Merrimac |\
	Methuen |\
	Middleton |\
	Newbury |\
	NEWBURYPORT |\
	North Andover |\
	PEABODY |\
	Rockport |\
	Rowley |\
	SALEM |\
	Salisbury |\
	Topsfield |\
	Wenham |\
	West Newbury |\
	LYNN |\
	Lynnfield |\
	Marblehead |\
	Nahant |\
	Saugus |\
	Swampscott"

	splitAssignTag(tenth, "Essex")

	eleventh = "Brimfield |\
	Holland |\
	Monson |\
	Palmer |\
	Wales |\
	Agawam |\
	Blandford |\
	Chester |\
	CHICOPEE |\
	East Longmeadow |\
	Granville |\
	Hampden |\
	HOLYOKE |\
	Longmeadow |\
	Ludlow |\
	Montgomery |\
	Russell |\
	Southwick |\
	SPRINGFIELD |\
	Tolland |\
	West Springfield |\
	WESTFIELD |\
	Wilbraham"
	splitAssignTag(eleventh, "Hampden")

	twelveth = "Ware |\
	Amherst |\
	Belchertown |\
	Chesterfield |\
	Cummington |\
	Easthampton |\
	Goshen |\
	Granby |\
	Hadley |\
	Hatfield |\
	Huntington |\
	Middlefield |\
	NORTHAMPTON |\
	Pelham |\
	Plainfield |\
	South Hadley |\
	Southampton |\
	Westhampton |\
	Williamsburg |\
	Worthington"
	splitAssignTag(twelveth, "Hampshire")

	thirteen ="Adams |\
	Alford |\
	Becket |\
	Cheshire |\
	Clarksburg |\
	Dalton |\
	Egremont |\
	Florida |\
	Great Barrington |\
	Hancock |\
	Hinsdale |\
	Lanesborough |\
	Lee |\
	Lenox |\
	Monterey |\
	Mount Washington |\
	New Ashford |\
	New Marlborough |\
	NORTH ADAMS |\
	Otis |\
	Peru |\
	PITTSFIELD |\
	Richmond |\
	Sandisfield |\
	Savoy |\
	Sheffield |\
	Stockbridge |\
	Tyringham |\
	Washington |\
	West Stockbridge |\
	Williamstown |\
	Windsor"
	splitAssignTag(thirteen, "Berkshire")

	fourteenth = "Ashfield |\
	Bernardston |\
	Buckland |\
	Charlemont |\
	Colrain |\
	Conway |\
	Deerfield |\
	Erving |\
	Gill |\
	Greenfield |\
	Hawley |\
	Heath |\
	Leverett |\
	Leyden |\
	Monroe |\
	Montague |\
	New Salem |\
	Northfield |\
	Orange |\
	Rowe |\
	Shelburne |\
	Shutesbury |\
	Sunderland |\
	Warwick |\
	Wendell |\
	Whately"
	splitAssignTag(fourteenth, "Franklin")

	return res

if __name__ == '__main__':
	x = getMapping()
	print(x)
