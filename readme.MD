# Note

### HSV Algorithm
* Transform from RGB to HSV
* Use only H-Hue and S-Saturation
* V-Value is for lightness, ignore it is better
* Use Correlation from 0 to 1 to calculate and represent the similarity between HS histograms.

### RDD
* Use RDD to divide the tasks by "time slot"
* in which a slot is 30 sec
* Then many local maximum similarity is got
* Should choose one largest local maximum
* the maximum is a tuple of 2 elements like "Tuple2<Long, Double>"
