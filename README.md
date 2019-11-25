# SpatioTemporal-Join

SpatioTemporal-Join was developed to interlink big data sets containing observations points with satellite images. The join is implemented using the [STARK](https://github.com/dbis-ilm/stark) library and hence as a prerequisite, it requires it as an sbt dependency.

Regarding the data, each observation point must contain the date it was observed (as yyyy-mm-dd hh:mm:ss) and its location as coordinates. The data set of the satellite images can contain multiple fields about the images but must contain the dates when they were captured, and their coverage as WKB in hex. The developed join interlinks the observation records with all the images that contain them and were captured the same day as the observations.


###  Build
As it was mentioned, the program requires the STARK library. In order to include it, build STARK from source and then import the jar as an sbt dependency in the build.sbt. Then to build SpatioTemporal-Join run
	
	sbt assembly
### Execution

	spark-submit --master local[*] --class spatiotemporal.PolarExperiment <path_to/sptemp_join-assembly-0.1.jar>   -s1 <satellite file> -ob <observations file> -s1_out <output path for s1> -ob_out <output path for observations>

*  **-s1 \<satellite file\>:** path to the file that contains the coverage and the timestamp of the images. Currently, only CSV files are supported.
*  **-ob \<observations file\>:**  path to the file that contains the observations. Currently, only CSV files are supported.
* **-s1_out \<output path for s1\>:** path for the output s1 file. This will be a CSV file containing the intersected images. The path must point to a non-existed directory.
*  **-ob_out \<output path for observations\>:** path for the output observation file.  This will be a CSV file containing the intersected points and the id of the images that were interlinked with. The path must point to a non-existed directory.



### Results 
The following gif visualises over time some observations of the north pole with the satellite images that captured them.  The visualization was implemented using the [Sextant](http://sextant.di.uoa.gr/) tool.<p  align="center">
<img  src="https://github.com/GiorgosMandi/SpatioTemporal-Join/blob/master/gif/spatiotemporal-join_2.gif">
</p>
