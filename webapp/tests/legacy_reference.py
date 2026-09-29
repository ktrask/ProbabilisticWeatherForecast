"""The legacy pictogram rules, frozen as a reference for the golden tests.

These are the getVSUP*Coordinate() functions of meteogram/plotMeteogram.py,
copied character for character when the Flask app and its matplotlib renderer
were removed - together with the filename lists of plot*VSUP() that their
return value indexes into. tests/test_vsup_golden.py holds the *-legacy
schemes in config/vsup.yaml to exactly these answers.

Do not edit them: they are the definition of "what the old app showed". Their
thresholds are read from this source by the golden tests, so a change here
would quietly move the reference instead of catching a change in the YAML.
"""

# The ensemble filename lists exactly as plot*VSUP() spelled them, index by
# index; the coordinate functions return a position in these.
VSUP_FILES = {
    "cloud": [
        "step1.png",
        "step2_mostly_clear.png",
        "step2_mostly_cloudy.png",
        "step3_sunny.png",
        "step3_light_clouds.png",
        "step3_medium_cloudy.png",
        "step3_cloud_max.png",
    ],
    "rain": [
        "step1_v2.png",
        "Stufe2_KaumRegen.png",
        "Stufe2_Regen.png",
        "Stufe3_KeinRegen.png",
        "Stufe3_leichterRegen.png",
        "Stufe3_MittlererRegen.png",
        "Stufe3_Starkregen.png",
    ],
    "wind": [
        "step1_v2.png",
        "Stufe2_kaumWind.png",
        "Stufe2_vielWind.png",
        "Stufe3_Windstille.png",
        "Stufe3_leichterWind.png",
        "Stufe3_starkerWind.png",
        "Stufe3_Sturm.png",
    ],
}

# What an index means: 0 is the vaguest glyph, 1-2 the middle, 3-6 certain.
LEVEL = {0: 1, 1: 2, 2: 2, 3: 3, 4: 3, 5: 3, 6: 3}

#Cloud cover is a percentage (0-100) as delivered by the Open-Meteo API.
def getVSUPCloudCoordinate(qdata):
    if qdata['ninety'] < 10:
        return(3)#no cloud
    if qdata['ten'] > 90:
        return(6)#all cloudy
    if qdata['ten'] > 50:
        return(5)#lot of clouds
    if qdata['ninety'] < 50:
        return(4)#light clouds
    if qdata['seventy_five'] < 70:
        return(1)#possibly light clouds
    if qdata['twenty_five'] > 30:
        return(2)#possibly strong clouds
    return(0)

#Precipitation is millimetres accumulated over the 6-hour step.
def getVSUPrainCoordinate(qdata):
    if qdata['ninety'] < 0.1:
        return(3)# no rain
    if qdata['ten'] > 2:
        return(6)#strong rain
    if qdata['ten'] > 1 and qdata['ninety'] < 2:
        return(5)#medium rain
    if qdata['ten'] > 1:
        return(2)#possibly rain
    if qdata['ninety'] < 1:
        return(4)#light rain
    if qdata['median'] > 1:
        return(2)#possibly strong or medium rain
    if qdata['seventy_five'] < 1.5:
        return(1)#possibly light rain
    return(0)

#Wind speed at 10 m in m/s.
def getVSUPWindCoordinate(qdata):
    if qdata['ninety'] < 3:#m/s
        return(3)#no wind
    if qdata['ten'] > 17.2:
        return(6)#storm
    if qdata['ten'] > 10 and qdata['ninety'] < 17.2:
        return(5)#strong wind
    if qdata['ninety'] < 10:
        return(4)#light wind
    if qdata['twenty_five'] > 10:
        return(2)#probably strong wind
    if qdata['seventy_five'] < 10:
        return(1)#probably light wind
    return(0)
