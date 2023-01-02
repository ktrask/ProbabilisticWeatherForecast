import optparse
import ecmwf.data as ecdata
from ecmwf.opendata import Client
from datetime import datetime


def download_grib(step_type):
    if step_type == "all":
        steps = list(range(0,361,6))
    else:
        steps = list(range(0,49,6))
    parameters = ['tcwv', 'tp', '10u', '10v', '2t']
    client = Client("ecmwf", beta=True)
    result = None
    for param in parameters:
        filename = f"forecast_{param}.grib"
        result = client.retrieve(
            #step=list(range(0,361,6)),
            step=steps,
            stream="enfo",
            type=['cf', 'pf'],
            levtype="sfc",
            param=[param],
            target=filename
        )
        #print(result.datetime)
    return result.datetime

def main():
    parser = optparse.OptionParser()
    parser.add_option(
        "-t", "--type",
        dest = "type",
        default = "small",
        help = "step type, small for two days, all for all days",
    )
    
    (options, args) = parser.parse_args()
    
    #print(options.type)
    print(download_grib(options.type))
    

if __name__ == "__main__":
    main()
