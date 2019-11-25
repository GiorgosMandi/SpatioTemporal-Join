# SpatioTemporal-Join

SpatioTemporal-Join was developed to interlink big data sets containing observations points with satellite images. The join is implemented using the [STARK](https://github.com/dbis-ilm/stark) library and hence as a prerequisite it requires it as an sbt dependency.

Regarding the data, each observation point must contain the date it was observed (as yyyy-mm-dd hh:mm:ss) and its location as coordinates. The data set of the satellite images can contain multiple fields about the images but must contain the dates when they were captured, and their coverage as WKB in hex. The developed join interlinks the observation records with all the images that contain them and were captured the same day as the observations.

### Results 
The following gif visualises over time some observations of the north pole with satellite images that captured them. 

<p  align="center">
<img  src="https://github.com/GiorgosMandi/SpatioTemporal-Join/blob/master/gif/spatiotemporal-join_2.gif">
</p>
