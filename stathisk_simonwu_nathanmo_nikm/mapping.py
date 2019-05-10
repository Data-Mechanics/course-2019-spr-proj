def getMapping():
	res = []

	def splitAssignTag(string, tag):
		for item in string.split('|'):
			res.append({'Town' : item.strip(), 'District' :tag})

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
	Yarmouth |\
	Acushnet |\
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
	Aquinnah |\
	Chilmark |\
	Edgartown |\
	Gosnold |\
	Oak Bluffs |\
	Tisbury |\
	West Tisbury |\
	Nantucket |\
	Bridgewater |\
	Carver |\
	Kingston |\
	Lakeville |\
	Marion |\
	Mattapoisett |\
	Middleborough |\
	Pembroke |\
	Plymouth |\
	Rochester |\
	Wareham"

	splitAssignTag(first, "1")


	second = "ATTLEBORO |\
	Easton |\
	Mansfield |\
	North Attleborough |\
	Norton |\
	Rehoboth |\
	Seekonk |\
	Ashland |\
	Framingham |\
	Holliston |\
	Hopkinton |\
	Natick |\
	Sherborn |\
	Wayland |\
	Avon |\
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
	East Bridgewater |\
	West Bridgewater |\
	BOSTON"

	splitAssignTag(second, "2")

	third = "Acton |\
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
	Brookline |\
	Wellesley |\
	Harvard |\
	Northborough |\
	Southborough |\
	Westborough"

	splitAssignTag(third, "3")

	fourth = "Easton |\
	Braintree |\
	Cohasset |\
	Holbrook |\
	QUINCY |\
	Weymouth |\
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
	Whitman |\
	BOSTON"

	splitAssignTag(fourth, "4")


	fifth = "Amesbury |\
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
	Dracut |\
	Dunstable |\
	Groton |\
	LOWELL |\
	North Reading |\
	Pepperell |\
	Tewksbury |\
	Tyngsborough |\
	Westford |\
	Wilmington"

	splitAssignTag(fifth, "5")

	sixth = "LYNN |\
	Lynnfield |\
	Marblehead |\
	Nahant |\
	Saugus |\
	Swampscott |\
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
	BOSTON |\
	CHELSEA |\
	REVERE |\
	Winthrop"

	splitAssignTag(sixth, "6")


	seventh = "Brimfield |\
	Holland |\
	Monson |\
	Palmer |\
	Wales |\
	Ware |\
	Ashby |\
	Townsend |\
	Bellingham |\
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
	WORCESTER"

	splitAssignTag(seventh, "7")

	eighth ="Adams |\
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
	Windsor |\
	Ashfield |\
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
	Whately |\
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
	Wilbraham |\
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
	Worthington |\
	Royalston"

	splitAssignTag(eighth, "8")

	# print(res)
	return res

if __name__ == '__main__':
	x = getMapping()
	print(x)
