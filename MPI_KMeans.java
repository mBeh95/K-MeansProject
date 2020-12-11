  /*
   * Author of revised version: Franklyn Pinedo
   * Author of new revised version: David Walker
   *
   * Adapted from Source Code in C of Tutorial/User's Guide for MPI by
   * Peter Pacheco.
   */

  import java.io.IOException;
  import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Scanner;
import java.io.*;
import java.io.FileWriter;

import mpi.* ;
// [IMPROVEMENT] 2 x 1D array
// 1st 1D array: number_of_clusters * number_of_points. End cluster with value -1
// offset [clusters] where offset[idx] contains the next avaiable slot to insert 

// current issue: have 2 nextAvailable, one for oldCluster and one for newCluster
  class MPI_KMeans {
  final static int tagFromMaster = 1;

    static public void main(String[] args) throws MPIException {
      MPI.Init(args) ;
      int myrank = MPI.COMM_WORLD.Rank( );
      int nprocs = MPI.COMM_WORLD.Size( );;

      if ( MPI.COMM_WORLD.Rank( ) == 0 ) {
        for (int i = 0; i < args.length; i++){
          System.out.println("arg["+i+"] = " +args[i]);
        }
        
    }
      
      int mtype = tagFromMaster;

      String filePath = "";
      String fileName = "FullData";
      int records = -1;
      int master = 0;

      try {records = getRecords(filePath, fileName);} catch (IOException e){System.err.println( e );} 
      double[][] points = new double[records][2];
      
      
    int xAttribute = 14;
		int yAttribute = 9;
      try{
		readRecords(filePath, fileName, points, xAttribute, yAttribute);
      } catch (IOException e){System.err.println( e );}

		int maxIterations = 100;
		int clusters = 7;

    long startTime = System.currentTimeMillis();
    

    double[][] means = new double[clusters][2];
		for(int i=0; i<means.length; i++) {
			means[i][0] = points[(int) (Math.floor((records*1.0/clusters)/2) + i*records/clusters)][0];
      means[i][1] = points[(int) (Math.floor((records*1.0/clusters)/2) + i*records/clusters)][1];
      // System.out.println("rank "+myrank+" has "+means[i][0]+" and "+means[i][1] + " at i is "+i);
    }
		
		int [] oldClusters = new int [clusters * points.length];
		int [] newClusters = new int [clusters * points.length];
		for (int i = 0; i < clusters * points.length; i++){
			oldClusters[i] = -2;
			newClusters[i] = -2;
		}
    // ArrayList<Integer>[] oldClusters = new ArrayList[clusters];
    // ArrayList<Integer>[] newClusters = new ArrayList[clusters];
    // ArrayList<Integer>[] tempClusters = new ArrayList[clusters];
    

		// for(int i=0; i<clusters; i++) {
		// 	oldClusters[i] = new ArrayList<Integer>(); // each centroid contains a list of points aka its "cluster group"
    //   newClusters[i] = new ArrayList<Integer>();
		// 	tempClusters[i] = new ArrayList<Integer>();
		// }
		int [] nextAvailable = new int [clusters];
		for (int i = 0 ; i < clusters; i++){
			nextAvailable[i] = (i * points.length);
			// if (myrank == master) System.out.println("[INITAL] nextAvailable of cluster "+i+" is "+(i * points.length));
		}
		formClusters(oldClusters, nextAvailable,means, points,0,points.length,myrank,false);
    
    int iterations = 0;

    // Showtime

    // points_stripe
    int points_remainder = points.length % nprocs;
    int points_my_stripe = ((points.length) / nprocs) + ( (myrank < points_remainder) ? 1: 0);
    int points_stripe = ((points.length) / nprocs);
    int points_first_i = (myrank < points_remainder) ? points_stripe * myrank + myrank : points_stripe * myrank + points_remainder;
    int points_last_i = points_first_i + points_my_stripe - 1;




		while(true) {
			// IMPORTANT: mpi java send and receive only understand 1D array...
			// if (myrank == master)System.out.println(" ---- ITERATION "+iterations+" ----");
      boolean debug = false;
      // if (iterations == 0) {debug = true;}
      // System.out.println("[rank "+myrank+"],"+"has maxIterations: "+maxIterations+", current iteration: "+iterations);
      updateMeans(oldClusters, means, points,myrank,debug);
      
			// System.out.println("rank "+myrank+" working on "+points_first_i+" to "+points_last_i);
			nextAvailable = new int [clusters];
			for (int i = 0 ; i < clusters; i++){
				nextAvailable[i] = (i * points.length);
				// if (myrank == master) System.out.println("[B4 formClusters] nextAvailable of cluster "+i+" is "+(i * points.length));
			}
			// if (myrank == master) System.out.println("FORMING CLUSTER");
      formClusters(newClusters, nextAvailable, means, points, points_first_i, points_last_i+1,myrank,debug);
      // convert newClusters to 1D
      if (myrank != master){
        // WORKER

				// send
				// if (myrank == 1){
				// 	for (int i = 0; i < newClusters.length;i++){
				// 		System.out.println("[rank 1] with "+newClusters[i]);
				// 	}
				// }
        MPI.COMM_WORLD.Send(newClusters, 0,newClusters.length, MPI.INT, master, mtype);
				// recv
				// int [] buffRecv = new int [clusters * points.length];
				newClusters = new int [newClusters.length];
				MPI.COMM_WORLD.Recv(newClusters,0,clusters * points.length,MPI.INT, master, mtype);
				// System.out.println("worker recv ");
        // for (int i = 0; i < clusters * points.length; i++){
				// 	newClusters[i] = buffRecv[i];
				// }
      }

      ////////////////////////////////////////////////////////////////////

      if (myrank == master){
        // MASTER
        // master recv from workers
        int [][] allBuff = new int [nprocs][];

        for (int rank = 1; rank < nprocs; rank++){
          int points_rank_stripe = ((points.length) / nprocs) + ( (rank < points_remainder) ? 1: 0);
          int [] buff = new int [clusters * points.length];
          // System.out.println("master expecting rank "+ rank +" to send " + points_rank_stripe + clusters + " bytes");
          MPI.COMM_WORLD.Recv( buff,0 ,clusters * points.length , MPI.INT, rank, mtype );
          allBuff[rank] = buff;
        }

				// master reduce

        for (int rank = 1; rank < nprocs; rank++){
          int [] temp = allBuff[rank];
          int i = 0;
					int ith_Cluster = 0;
          while (i < temp.length && ith_Cluster < clusters){
            if (temp[i] != -1 && temp[i] != -2){
							int idx_insert = nextAvailable[ith_Cluster];
							newClusters[idx_insert] = temp[i]; 
							nextAvailable[ith_Cluster] += 1 ;
            } else if (temp[i] == -1) {
              ith_Cluster += 1;
            } 
            i++;
					}
				}
				// lock
				for (int curr_idx = 0; curr_idx < means.length;curr_idx++){
					int idx_insert = nextAvailable[curr_idx];
					newClusters[idx_insert] = -1;
				}
        // then master send
        for (int rank = 1; rank < nprocs; rank ++) {
					// System.out.println("master sending: "+clusters * points.length);
          MPI.COMM_WORLD.Send(newClusters, 0,clusters * points.length, MPI.INT, rank, mtype);
        }
      }
      
			iterations++;

			// if(iterations > maxIterations || checkEquality(oldClusters, newClusters)) 
			if(iterations > maxIterations ){
				break;
			}
			else
				{
					resetClusters(oldClusters, newClusters,clusters, points.length,nextAvailable);

					nextAvailable = new int [clusters];
					for (int i = 0 ; i < clusters; i++){
						nextAvailable[i] = (i * points.length);
					}
					newClusters = new int [clusters * points.length];
					for (int i = 0; i < newClusters.length;i++){
						newClusters[i] = -2;
					}

					// if (myrank == 1) System.out.println("oldClusters");
					
				}
		}

		// Display the output
    if (myrank == 0) {
      displayOutput(oldClusters, points,clusters);
		  System.out.println("\nIterations taken = " + iterations);

      // sc.close();
      long endTime = System.currentTimeMillis();
      System.out.println("time taken is " + ((endTime - startTime) / 1000)+" second(s)");
    }
    if (myrank ==0 ){
      System.out.println("DONE");
    }
      MPI.Finalize();
    }
    static int getRecords(String filePath, String fileName) throws IOException {
      int records = 0;
      BufferedReader br = new BufferedReader(new FileReader(filePath + fileName + ".csv"));
      while (br.readLine() != null)
        records++;
  
      br.close();
      return records;
    }
  
    static void readRecords(String filePath, String fileName, double[][] points, int xAttribute, int yAttribute) throws IOException {
      BufferedReader br = new BufferedReader(new FileReader(filePath + fileName + ".csv"));
      String line;
      int i = 0;
      while ((line = br.readLine()) != null) {
        points[i][0] = Double.parseDouble(line.split(",")[xAttribute]);
        points[i++][1] = Double.parseDouble(line.split(",")[yAttribute]);
      }
  
      br.close();
    }
  
    static void sortPointsByX(double[][] points) {
      double[] temp;
  
      // Bubble Sort
      for(int i=0; i<points.length; i++)
          for(int j=1; j<(points.length-i); j++)
        if(points[j-1][0] > points[j][0]) {
            temp = points[j-1];
            points[j-1] = points[j];
            points[j] = temp;
        }
    }
  
    static void updateMeans(int [] clusterList, double[][] means, double[][] points,int myrank,boolean debug) {
			double totalX = 0,totalY;

			totalY = totalX;
			totalX = totalY;

			int j = 0; // keep track on current element in clusterList
			int ith_Cluster = 0;
			int count = 0;
				while (j < clusterList.length && ith_Cluster < means.length){
					if (clusterList[j] != -1 && clusterList[j] != -2){
						
						int index = clusterList[j];
						totalX += points[index][0];
						totalY += points[index][1];
						count ++;
						
					} else if (clusterList[j] == -1) {
						// next cluster
						means[ith_Cluster][0] = totalX/count;
						means[ith_Cluster][1] = totalY/count;
						
						// reset params
						count = 0;
						totalX = 0.0;
						totalY = 0.0;
						
						ith_Cluster += 1;
					}
					j++;
				}
    }
  
    static void formClusters(int [] clusterList, int [] nextAvailable,double[][] means, double[][] points, int start, int end, int myrank,boolean run) {
      double distance[] = new double[means.length];
      double minDistance = Integer.MAX_VALUE;
			
			int minIndex = 0;
			// if (myrank == 1){
			// 	System.out.println("[rank 1] handling points "+start+" to "+end);
			// }
      for(int idx = start; idx < end; idx++) {
        minDistance = Integer.MAX_VALUE;
        for(int j=0; j< means.length; j++) {
          distance[j] = Math.sqrt(Math.pow((points[idx][0] - means[j][0]), 2) + Math.pow((points[idx][1] - means[j][1]), 2));
          
          if(distance[j] < minDistance) {
            minDistance = distance[j];
            minIndex = j;
          }
          
        }
				int idx_insert = nextAvailable[minIndex];
				clusterList[idx_insert] = idx;
				// if (myrank == 1) System.out.println("[rank 1] cluster "+minIndex+" adding point number "+idx + " to point "+idx_insert);
				nextAvailable[minIndex] += 1;
			}
			// lock
			for (int i = 0; i < means.length;i++){
				int idx_insert = nextAvailable[i];
				clusterList[idx_insert] = -1;
			}
    }
  
    static boolean checkEquality(ArrayList<Integer>[] oldClusters, ArrayList<Integer>[] newClusters) {
      for(int i=0; i<oldClusters.length; i++) {
        // Check only lengths first
        if(oldClusters[i].size() != newClusters[i].size())
          return false;
  
        // Check individual values if lengths are equal
        for(int j=0; j<oldClusters[i].size(); j++)
          if(oldClusters[i].get(j) != newClusters[i].get(j))
            return false;
      }
  
      return true;
    }
  
    static void resetClusters(int [] oldClusters, int [] newClusters, int clusters, int num_points, int [] nextAvailable) {
				
				for (int i = 0; i < newClusters.length; i++){
					oldClusters[i] = newClusters[i];
				}


				
    }
    static void displayMeans (int [][] arr,String outputFileName){
      try {
        FileWriter myWriter = new FileWriter(outputFileName+".txt");
 
     for(int i=0; i< arr.length; i++) {
        myWriter.write(arr[i][0] + " " + arr[i][1]);
        myWriter.write("\n");
    }
 
       myWriter.close();
     } catch (Exception e)
     {System.out.println("An error occurred.");e.printStackTrace();}
    }
    static void displayOutput(int [] clusterList, double[][] points, int clusters) {
      // File myObj = new File("output.txt");
      try {
         FileWriter myWriter = new FileWriter("filename.txt");
				 String clusterOutput = "\n\n[";
				 int curr_idx = 0;
				 int ith_Cluster = 0;
				 while (curr_idx < clusterList.length && ith_Cluster < clusters){
					//  System.out.println("[DISPLAY] curr element "+clusterList[curr_idx]);
					 if (clusterList[curr_idx] != -1 && clusterList[curr_idx] != -2){
							int index = clusterList[curr_idx];
					 		// System.out.println("[DISPLAY] adding point idx "+index);
							clusterOutput += "(" + points[index][0] + ", " + points[index][1] + "), ";
					 }
					 else if (clusterList[curr_idx] == -1) {
						 // next cluster
						 myWriter.write(clusterOutput.substring(0, clusterOutput.length()-2) + "]");
						 clusterOutput = "\n\n[";
						 ith_Cluster += 1;
						//  System.out.println("[DISPLAY] next cluster: "+ith_Cluster);

					 }
					curr_idx++;
				 }
  
        myWriter.close();
      } catch (Exception e)
      {System.out.println("An error occurred.");e.printStackTrace();}
    }

  }