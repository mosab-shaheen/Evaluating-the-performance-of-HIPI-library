# Evaluating-the-performance-of-HIPI-library
Evaluating the performance of HIPI library on Image Segmentation Task in Different Hadoop Configurations

# Abstract
The available media nowadays is continuously increasing. Huge amount of images from different sources like social media, satellite images, etc. can be used for different applications. To be able to handle this vast amount of data, parallel and distributed frameworks come into the picture.
Hadoop is a widely used framework for distributed processing of big data. While Hadoop showed good performance, it suffers from large number of small size files. Hadoop Image Processing Interface (HIPI) library solved this problem when working with images. In this work, we will compare HIPI with sequence files and basic Hadoop and see the improvement gained by using it, also we will use different configurations of Hadoop to see how we can get better results. We will evaluate
the performance on segmentation/clustering tasks over satellite images. 

# Block diagram of the work 
<img width="432" height="712" alt="image" src="https://github.com/user-attachments/assets/c070dd42-3562-4801-98be-075ab5b82afe" />

# Clustering using HIPI and K-means 
<img width="382" height="681" alt="image" src="https://github.com/user-attachments/assets/c7b89256-01ab-4ab4-87de-024164470b51" />


# Configuration
The configuration files can be found in the "Conf.rar" file. The important settings are:
*  mapreduce.map.memory.mb: determines the maximum physical memory that is needed to run one map task. If it is exceeded, usually you get
"Container is running beyond physical memory limits". This configuration determines the number of Map Tasks that can run in parallel as Hadoop will see how much memory available and how much memory each Map Task needs. Same thing applies to mapreduce.reduce.memory.mb for Reduce Tasks.<br>
*  mapreduce.map.java.opts: determines the maximum heap memory assigned to a Map Task. If it is exceeded, usually you get "java. lang.
OutOfMemoryError: Java heap space". Same thing applies to mapreduce.reduce.java.opts for ReduceTasks. <br>
*  yarn.scheduler.minimum-allocation-mb: minimum memory for a container.<br>
*  yarn.scheduler.maximum-allocation-mb: maximum memory for a container. <br>
*  yarn.nodemanager.resource.memory-mb: maximum memory available in a node.<br>
*  yarn.nodemanager.vmem-pmem-ratio: the ratio between physical memory and virtual memory. We talked about the physical memory before; for a Map Task the virtual memory is calculated by multiplying mapreduce.map.memory.mb and yarn.nodemanager.vmem-pmem-ratio. Same thing applies to Reduce Tasks.<br>
*  yarn.nodemanager.vmem-check-enabled: sometimes the virtual memory exceeds the limit defined by the ratio mentioned above. This may kill the container and give a message like "1.1gb of 1.0gb virtual memory used. Killing container.", this can happen because of the aggressive allocation of the memory by the operation system. To stop checking, if the virtual memory exceeds the ratio, you can use this option.<br>
*  yarn.nodemanager.resource.cpu-vcores: specifies the number of CPU cores the node has.<br>

# Implemenation
The code can be found in the HIPI* folders.

# Data
The data are satellite images of multilayers and are in TIFF format. They were provided by the Bhaskaracharya Institute for Space Applications and Geoinformatics, Gandhinagar, India


# Sample output of a segmented image
<img width="627" height="372" alt="image" src="https://github.com/user-attachments/assets/13362423-1bfc-467f-a4f6-ab70d068b7d4" />


# Results 

Check the main research article here [Evaluating HIPI Performance on Image Segmentation Task in Different Hadoop Configurations](https://www.irjet.net/archives/V4/i6/IRJET-V4I607.pdf)
