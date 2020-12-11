import edu.uw.bothell.css.dsl.MASS.Agents;
import edu.uw.bothell.css.dsl.MASS.MASS;
import edu.uw.bothell.css.dsl.MASS.Places;
import edu.uw.bothell.css.dsl.MASS.logging.LogLevel;
import MASS.*; 
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
import java.io.*;
import java.io.FileWriter;
public class KMeans {
	public static void main(String args[]) throws IOException {
		Scanner sc = new Scanner(System.in);
		String filePath = "";
		// System.out.print("Enter the name of the CSV file: ");
		String fileName = "short";

		// Open the file just to count the number of records
		int records = getRecords(filePath, fileName);                   //    <----- DO THIS
		
		// System.out.print("Enter the index of the X-attribute: ");
		int xAttribute = 14;
		// System.out.print("Enter the index of the Y-attribute: ");
		int yAttribute = 9;
    int nAgents = 4;
		// Open file again to read the records
		double[][] points = new double[records][2];
		//readRecords(filePath, fileName, points, xAttribute, yAttribute); //    <----- DO THIS

		// Sort the points based on X-coordinate values
		// sortPointsByX(points);

		// Input the number of iterations
		// System.out.print("Enter the maximum number of iterations: ");
		int maxIterations = 10;
        
		// Input number of clusters
		// System.out.print("Enter the number of clusters to form: ");
		int clusters = 5;
    MASS.setLoggingLevel( LogLevel.DEBUG );
    MASS.init();
		Places land = new Places( 1, "Land", null, records );
    
		long startTime = System.currentTimeMillis();

    Agents nomads = new Agents( 2, "Nomad", null, land, nAgents );

    // initially send out the index assignment for both points and clusters
    for (int agent_idx = 0; agent_idx < nAgents; agent_idx++){
      
      int points_remainder = points.length % nAgents;
      int points_my_stripe = ((points.length) / nAgents) + ( (agent_idx < points_remainder) ? 1: 0);
      int points_stripe = ((points.length) / nAgents);
      int points_first_i = (agent_idx < points_remainder) ? points_stripe * agent_idx + agent_idx : points_stripe * agent_idx + points_remainder;
      int points_last_i = points_first_i + points_my_stripe - 1;

      int clusters_remainder = clusters % nAgents;
      int clusters_my_stripe = (clusters / nAgents) + ( (agent_idx < clusters_remainder) ? 1: 0);
      int clusters_stripe = (clusters / nAgents);
      int clusters_first_i = (agent_idx < clusters_remainder) ? clusters_stripe * agent_idx + agent_idx : clusters_stripe * agent_idx + clusters_remainder;
      int clusters_last_i = clusters_first_i + clusters_my_stripe - 1;

      StringBuilder str_assignment = new StringBuilder();
      str_assignment.append(new String(points_first_i));str_assignment(',');
      str_assignment.append(new String(points_last_i));str_assignment(',');
      str_assignment.append(new String(clusters_first_i));str_assignment(',');
      str_assignment.append(new String(clusters_last_i));

      MASS.getMessagingProvider().sendAgentMessage(agent_idx,str_assignment);
    }

    StringBuilder str_arg = new StringBuilder();
    str_arg.append(filePath); str_arg.append(',');
    str_arg.append(fileName); str_arg.append(',');
    str_arg.append(new String(xAttribute)); str_arg.append(',');
    str_arg.append(new String(yAttribute));str_arg.append(',');
    str_arg.append(new String(records));str_arg.append(',');
    str_arg.append(new String(clusters));



    nomads.callAll( Nomad.initialization_, str_arg );
    
    // nomads.callAll( Nomad.decideNewPosition_, (Object)null );


    /**
     * create a means_Place where it is means.length x 2
     * at each tile of Place call a function that calculate 
     *  points[(int) (Math.floor((records*1.0/clusters)/2) + i*records/clusters)][i]; 
     *  where i is the index of the current tile
     */

		int iterations = 0;

		// Showtime
		while(true) {
			// updateMeans(oldClusters, means, points,false);
      // formClusters(newClusters, means, points,false);
      nomads.callAll( Nomad.updateMean_, null );
      nomads.callAll( Nomad.formingCluster_, null );

			iterations++;

			if(iterations > maxIterations)
				break;
			else
				resetClusters(oldClusters, newClusters); // <---- NOT DONE YET
		}
		// Display the output
		// System.out.println("\nThe final clusters are:");
		displayOutput(oldClusters, points);
		System.out.println("\nIterations taken = " + iterations);

		sc.close();
		long endTime = System.currentTimeMillis();
  	System.out.println("time taken is " + ((endTime - startTime) / 1000)+" second(s)");
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

	static void updateMeans(ArrayList<Integer>[] clusterList, double[][] means, double[][] points, boolean debug) {
    // clusterList.length = number of clusters
		double totalX = 0;
    double totalY = 0;
    /**
     * for every centroid, calculate total x-coordinate and y-coordinate 
     * then store those to totalX and totalY. 
     * Then calculate the average x-coordinate and y-coordinate then assign it to the current centroid coordinate
     */
    /**
     * OPTIMIZE MPI: allocate a certain number of centroids to each worker.
     * each node calculate totalX and totalY of each assigned centroid.
     * 
     * at the end of outter loop, send the info back to master
     */
		for(int i=0; i<clusterList.length; i++) {
			totalX = 0;
			totalY = 0;
			for(int index: clusterList[i]) {
				totalX += points[index][0];
        totalY += points[index][1];
        if (debug){
          if (i == 0  || i == 1) System.out.println("cluster "+ i +" has point "
          + index +", with pointX: "+points[index][0] + ", pointY: "+points[index][1]);
        }
      }
			means[i][0] = totalX/clusterList[i].size();
      means[i][1] = totalY/clusterList[i].size();
      // if (debug) System.out.println("means of "+i+" is "+means[i][0]+" / "+means[i][1]);
		}
	}

	static void formClusters(ArrayList<Integer>[] clusterList, double[][] means, double[][] points, boolean debug) {
		double distance[] = new double[means.length];
		double minDistance = Integer.MAX_VALUE;
    int minIndex = 0;
    
		for(int i=0; i<points.length; i++) {
			minDistance = Integer.MAX_VALUE;
			for(int j=0; j<means.length; j++) {
				distance[j] = Math.sqrt(Math.pow((points[i][0] - means[j][0]), 2) + Math.pow((points[i][1] - means[j][1]), 2));
				if(distance[j] < minDistance) {
					minDistance = distance[j];
					minIndex = j;
        }
      }
			clusterList[minIndex].add(i); // with the current point i, point i will be assigned to closest centroid
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

	static void resetClusters(ArrayList<Integer>[] oldClusters, ArrayList<Integer>[] newClusters) {
		for(int i=0; i<newClusters.length; i++) {
			// Copy newClusters to oldClusters
			oldClusters[i].clear();
			for(int index: newClusters[i])
				oldClusters[i].add(index);

			// Clear newClusters
			newClusters[i].clear();
		}
	}
	static void displayOutput(ArrayList<Integer>[] clusterList, double[][] points) {
		// File myObj = new File("output.txt");
		try {
			 FileWriter myWriter = new FileWriter("filename.txt");

		for(int i=0; i<clusterList.length; i++) {
			String clusterOutput = "\n\n[";
			for(int index: clusterList[i]) {
				clusterOutput += "(" + points[index][0] + ", " + points[index][1] + "), ";
	}
         	myWriter.write(clusterOutput.substring(0, clusterOutput.length()-2) + "]");
		}

			myWriter.close();
    } catch (Exception e)
    {System.out.println("An error occurred.");e.printStackTrace();}
	}
}