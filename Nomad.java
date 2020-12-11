import java.io.Serializable;
import java.util.Date;
import java.util.Random;

import edu.uw.bothell.css.dsl.MASS.Agent;
import edu.uw.bothell.css.dsl.MASS.MASS;
import edu.uw.bothell.css.dsl.MASS.annotations.OnArrival;
import edu.uw.bothell.css.dsl.MASS.annotations.OnCreation;
import edu.uw.bothell.css.dsl.MASS.annotations.OnMessage;
import edu.uw.bothell.css.dsl.MASS.matrix.MatrixUtilities;
import edu.uw.bothell.css.dsl.MASS.messaging.MessageDestination;

// [IF-POSSIBLE][REQUIRE] start and end clusters for updateMean

public class Nomad extends Agent implements Serializable{
	public static final int GET_DATA = 42;
  private int numAgents;

  private int records;
  private int clusters;
  private double[][] points;
  private double[][] means;
  private int [] oldClusters;
  private int [] newClusters;
  private int [] nextAvailable;

  private int points_first_i;
  private int points_last_i;
  private int clusters_first_i;
  private int clusters_last_i;
  /**
	 * Is the default constructor.
	 */
	public Nomad( ) {
		super( );
  }
  public Nomad( Object arg ) {

		MASS.getLogger( ).debug( "agent(" + getAgentId( ) + ") was born." );

		numAgents = (int) arg;

  }
      // function identifiers
      public static final int readRecords_ = 0;
      public static final int formCluster_ = 1;
      public static final int updateMean_  = 2;



      public Object callMethod( int funcId, Object args ) {
        // I am a dispatcher
          switch ( funcId ) {
          case initialization_: return initialization( args );
          case formingCluster_: return formingCluster( args );
          case updateMean_: return updateMean( args );

          }
          return null;
      }
      private Object initialization (Object args){
        String [] arr = ((String) args).split(",");
        String filePath = arr[0];
        String fileName = arr[1];
        int xAttribute = Integer.parseInt(arr[2]);
        int yAttribute = Integer.parseInt(arr[3]);

       records = Integer.parseInt(arr[4]);
       clusters = Integer.parseInt(arr[5]);
       points = new double[records][2];

       BufferedReader br = new BufferedReader(new FileReader(filePath + fileName + ".csv"));
        String line;
        int i = 0;
        while ((line = br.readLine()) != null) {
          points[i][0] = Double.parseDouble(line.split(",")[xAttribute]);
          points[i++][1] = Double.parseDouble(line.split(",")[yAttribute]);
        }
    
        br.close();
        // initializing mean
		    means = new double[clusters][2];
        initialUpdateMean();
        // create 2 clusters
        oldClusters = new int [clusters * points.length];
        newClusters = new int [clusters * points.length];
        // initialize 2 clusters
        for (int idx = 0; idx < clusters * points.length; idx++){
          oldClusters[idx] = -2;
          newClusters[idx] = -2;
        }
        // initialize nextAvailable
        nextAvailable = new int [clusters];
        for (int idx = 0 ; idx < clusters; idx++){
          nextAvailable[idx] = (idx * points.length);
        }
        // run first formCluster

        // [PAUSE]
        // [REQUIRE] start and end points for formCluster                  [DONE]
        
        formClusters(oldClusters,nextAvailable,means,points,points_first_i,points_last_i);
        // then broadcast your message                                     [DONE]
				MASS.getMessagingProvider().sendAgentMessage( MessageDestination.ALL_AGENTS, oldClusters );
        // OnMessage recv a package type int [] send_cluster               [DONE]
        // then 'reduce' it                                                [DONE]

        return null;
      }
      public void initialUpdateMean(){
        for(int i=0; i<means.length; i++) {
          means[i][0] = points[(int) (Math.floor((records*1.0/clusters)/2) + i*records/clusters)][0];
          means[i][1] = points[(int) (Math.floor((records*1.0/clusters)/2) + i*records/clusters)][1];
        }

      }
      public Object updateMean(Object args){
        double totalX = 0,totalY;
        totalY = totalX;
        totalX = totalY;

        int start_cluster_index = (clusters_first_i * points.length);
        int j = start_cluster_index; // keep track on current element in clusterList
        
        int ith_Cluster = clusters_first_i;
        int end_cluster = clusters_last_i;

        int count = 0;
          while (j < oldClusters.length && ith_Cluster <= end_cluster){
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
          //  convert to [int cluster_idx01, double x_coord_idx01, double y_coord_idx01, int cluster_idx02, double x_coord_idx02, double y_coord_idx02,...]
          double [] send_means = new double [(clusters_last_i - clusters_first_i + 1) * 2];
          int curr_cluster = clusters_first_i;
          for (int curr = 0;curr < send_means.length; curr += 3 ){
            if (curr_cluster <= end_cluster){

              send_means[curr] = curr_cluster;
              curr+=1;
              send_means[curr] = means[curr_cluster][0];
              curr+=1;
              send_means[curr] = means[curr_cluster][1];

              curr_cluster++;
            }
          }
				MASS.getMessagingProvider().sendAgentMessage( MessageDestination.ALL_AGENTS, send_means );
        return null;
      }
      public Object formingCluster (Object arg){
        nextAvailable = new int [clusters];
        for (int i = 0 ; i < clusters; i++){
          nextAvailable[i] = (i * points.length);
        }
        formClusters(newClusters,nextAvailable,means,points,points_first_i,points_last_i);
				MASS.getMessagingProvider().sendAgentMessage( MessageDestination.ALL_AGENTS, newClusters );
        return null;
      }
      public void formClusters(int [] clusterList, int [] nextAvailable,double[][] means, double[][] points, int start, int end) {
        // INCOMPLETE
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
  @OnMessage
  public void receivePayLoadMessage(String args){
    String [] indexes = args.split(",");
    points_first_i =   indexes[0];
    points_last_i =    indexes[1];
    clusters_first_i = indexes[2];
    clusters_last_i =  indexes[3];
  }
  public void receivePayLoadMessage(int [] recv_cluster){
    int i = 0;
    int ith_Cluster = 0;
    while (i < recv_cluster.length && ith_Cluster < clusters){
      if (recv_cluster[i] != -1 && recv_cluster[i] != -2){
        int idx_insert = nextAvailable[ith_Cluster];
        newClusters[idx_insert] = recv_cluster[i]; 
        nextAvailable[ith_Cluster] += 1 ;
      } else if (recv_cluster[i] == -1) {
        ith_Cluster += 1;
      } 
      i++;
    }
    // lock
    for (int curr_idx = 0; curr_idx < means.length;curr_idx++){
      int idx_insert = nextAvailable[curr_idx];
      newClusters[idx_insert] = -1;
    }
  }
  public void receivePayLoadMessage(double [] recv_means){
  for (int idx = 0; idx < recv_means.length;idx += 3){
    int curr_cluster = (int) recv_means[0];
    double x_coord =         recv_means[1];
    double y_coord =         recv_means[2];
    means[curr_cluster][0] = x_coord;
    means[curr_cluster][1] = y_coord;
  }
  }
}
