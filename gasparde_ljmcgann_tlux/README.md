Project 3
================================================================================
This README is for the purpose of explaining how to run execute.py
and our visualization rendered using Flask. For more details about the
methodology read our Report.pdf which can be found this directory.

### Dependencies
Besides the dependencies needed by **execute.py** outline by the Data Mechanics Course, 
we need to install the following packages:

**Shapely**
```
pip install Shapely 
```
On Windows you need to download and install the wheel file which can be found here:  [Shapely.whl](http://www.lfd.uci.edu/~gohlke/pythonlibs/#shapely)

**Scipy**
````
pip install scipy
````

**Rtree**

Follow instructions found [here](http://toblerity.org/rtree/install.html#)

**tqdm**
````
pip install tqdm
````
**Scipy**
````
pip install scipy
````
**Flask**
````
pip install Flask
````
## Run excute.py
To run simply enter:
```
python execute.py gasparde_ljmcgann_tlux
```
and to run using trial mode enter:
```
python execute.py gasparde_ljmcgann_tlux --trial
```
### Visualization
To run the visualization, one first needs to run the
execute.py script so that Flask has the data within
the MongoDB cluster needed to render the website. The
visualization will also work if you run trial mode, but
you will be limited only to the neighborhood of Allston.
To render the visualization simply run the app.py file found in the project3 
subdirectory. This should render html locally on your machine.

Note: We've added some redundant code in the project3 folder in the 
kmeans.py file. This was because we wanted to make sure
our app.py was able to reference the necessary
code to run the kmeans and trying to reference
optimize.py in the parent directory has been known to
be buggy.