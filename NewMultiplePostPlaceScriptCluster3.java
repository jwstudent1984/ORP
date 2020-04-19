package edu.usc.enl.cacheflow.scripts.vcrib;

import edu.usc.enl.cacheflow.algorithms.migration.rmigration3.RMigrateVMStartPartition3;
import edu.usc.enl.cacheflow.algorithms.migration.rmigration3.RMigrateVMStartPartition4;
import edu.usc.enl.cacheflow.algorithms.placement.switchselectionpartition.ThreadMinTrafficSwitchSelection;
import edu.usc.enl.cacheflow.algorithms.migration.*;
import edu.usc.enl.cacheflow.algorithms.placement.AssignerCluster;
import edu.usc.enl.cacheflow.algorithms.placement.NoAssignmentFoundException;
import edu.usc.enl.cacheflow.algorithms.placement.switchselectionpartition.TrafficAwareSwitchSelection;
import edu.usc.enl.cacheflow.model.Assignment;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.WriterSerializableUtil;
import edu.usc.enl.cacheflow.model.factory.*;
import edu.usc.enl.cacheflow.model.factory.FileFactory.EndOfFileCondition;
import edu.usc.enl.cacheflow.model.rule.Cluster;
import edu.usc.enl.cacheflow.model.rule.MatrixRuleSet;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Link;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchhelper.MemorySwitchHelper;
import edu.usc.enl.cacheflow.model.topology.switchmodel.MemorySwitch;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.file.SaveFileProcessor;
import edu.usc.enl.cacheflow.processor.flow.classifier.OVSClassifier;
import edu.usc.enl.cacheflow.processor.flow.classifier.ThreadTwoLevelTrafficProcessor;
import edu.usc.enl.cacheflow.processor.network.InformOnRestart;
import edu.usc.enl.cacheflow.processor.network.RunFlowsOnNetworkProcessor2;
import edu.usc.enl.cacheflow.algorithms.Placer;
import edu.usc.enl.cacheflow.algorithms.PostPlacer;
import edu.usc.enl.cacheflow.processor.network.VMStartMigrateRandomProcessor;
import edu.usc.enl.cacheflow.processor.partition.SourcePartitionFinder;
import edu.usc.enl.cacheflow.processor.rule.aggregator.RemoveEqualIDProcessor;
import edu.usc.enl.cacheflow.util.CollectionPool;
import edu.usc.enl.cacheflow.util.Util;

import java.io.*;
import java.text.DecimalFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.logging.Level;

import org.apache.tools.ant.util.FileUtils;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 1/1/12
 * Time: 11:18 AM
 * To change this template use File | Settings | File Templates.
 */
public class NewMultiplePostPlaceScriptCluster3 {
    public static Map<Partition, Map<Switch, Switch>> NowpostPlaceResult;
	public static Map<Switch,LinkedList<Switch>> findpath = new HashMap<Switch,LinkedList<Switch>>();
	public static Map<Switch, HashSet<Rule>> TempRules = new HashMap<Switch, HashSet<Rule>>();
	public static Map<Switch, HashSet<Integer>> TempRulesid = new HashMap<Switch, HashSet<Integer>>();
	public static int runNumber;
	public static int VMNumber=20;
	public static int frequent = 0;
	public static boolean debug = false;
	public static long originaltraffic = 0;
	public static Map<Switch, Integer> RedirectionruleNumber = new HashMap<Switch, Integer>();
		//public static Map<Switch, Set<Partition>> placementpartition = new HashMap<Switch, Set<Partition>>();
	public static Map<Partition, Switch> PlaceResult = new HashMap<Partition, Switch>();
//    public static Map<Long, Partition> debugPartitionMap;
	public static Map<Integer, Partition> Partitionid = new HashMap<Integer, Partition>();
	public static Map<String, Switch> Switchid = new HashMap<String, Switch>();
	//public static LinkedList<HashSet<Rule>> rulearray= new  LinkedList<HashSet<Rule>>();
	protected static long migrationStart;
	public static Map<Partition, Collection<Rule>> MapPartitionrules = new HashMap<Partition, Collection<Rule>>();
	public static Map<Integer, Collection<Integer>> MapPartitionrulesid = new HashMap<Integer, Collection<Integer>>();
	//public static Map<Integer, HashSet<Integer>> Maprule = new HashMap<Integer, HashSet<Integer>>();
	//public static LinkedList<HashSet<Rule>> rulearray= new  LinkedList<HashSet<Rul
    public static void main(String[] args) throws Exception {
    	
    	long ProcessStart = System.currentTimeMillis();
        
    	//while(true){
    		
    	long ProcessEnd = System.currentTimeMillis();	
    	System.out.println("endTime"+ProcessEnd);
      	String parentFolder = "d://vCRIB-master//vCRIB-master//examples";
        //int randomSeedIndex = Integer.parseInt(args[0]);
    	//这个randomSeedindex的具体含义还是不清楚
    	String topologyFolderPath = parentFolder + "//topology";
        //Util.threadNum = Integer.parseInt(args[0]);
    	Util.threadNum = 1;
        //这个randomseedindex是什么意思？
        //int randomSeedIndex = Integer.parseInt(args[1]);
        int randomSeedIndex = 0;
        //String maxTopology = args[2];
        //File topologyFolder = new File(args[3]);
        String maxTopology = "21.txt";
        //topology的文件地址
        //File topologyFolder = new File(args[3]);
        File topologyFolder = new File(topologyFolderPath);
        //String flowFilePath = parentFolder + "//imcflowspecs0.5//classbench_20_0.txt";
        //String flowFilePath_1 = parentFolder + "//imcflowspecs0.5//classbench_20_0.txt";
        String flowFilePath = parentFolder + "//imcflowspecs0.5//classbench_84649_1.txt";
        String flowFilePath_1 = parentFolder + "//imcflowspecs0.5//classbench_84649_1.txt";
        //File flowFile = new File(args[4]);
        File flowFile = new File(flowFilePath);
        File flowFile_1 = new File(flowFilePath_1);
        //String inputvmFolder = parentFolder + "//inputvmFolder0.5//classbench_20_1.txt";
        String inputvmFolder = parentFolder + "//inputvmFolder0.5//classbench_84649_1.txt";
        File vmFile = new File(inputvmFolder);
        //String PartitonFolder = parentFolder + "//partitions//classbench_20_1.txt";
        String PartitonFolder = parentFolder + "//partitions//classbench_84649_1.txt";
        File partitionFile = new File(PartitonFolder);
        String outputFolder = parentFolder+ "//1//3";
        String assignmentFolder=parentFolder+ "//1//assignment";
        //File clusterFolder = new File(args[9]);
        File clusterFolder = new File(parentFolder+ "//1//runflows.txt");


        Map<String, Object> parameters = new HashMap<String, Object>();
        //run(flowsFile, partitionFile, topologyFolder, parameters, maxTopology, randomSeedIndex, outputFolder);
        //laundry stuff
        Util.setRandom(randomSeedIndex);
        Util.logger.setLevel(Level.WARNING);
        {
            File outputFolderFile = new File(outputFolder);
            outputFolderFile.mkdirs();
        }


        //load data
        final UnifiedPartitionFactory partitionFactory = new UnifiedPartitionFactory(new FileFactory.EndOfFileCondition(), true, new HashSet<Rule>());
        //final UnifiedPartitionFactory partitionFactory = new UnifiedPartitionFactory(new FileFactory.EndOfFileCondition(), new HashSet<Rule>());
        List<Partition> partitions = Util.loadFileFilterParam(partitionFactory, partitionFile.getPath(), parameters, new LinkedList<Partition>(), "(partition|rule)\\..*");
        int k=0;
        
        for(Partition partition:partitions) {
        	MapPartitionrules.put(partition, partition.getRules());
        	
        	Partitionid.put(partition.getId(), partition);
        	//Partitionchid_1.put(partition, k);
        	HashSet<Integer> idarray = new HashSet<Integer>();
        	for(Rule rule:partition.getRules()) {
        		idarray.add(rule.getId());
        	}
        	//Maprule.put(partition.getId(), idarray);
        	k++;
        	MapPartitionrulesid.put(partition.getId(), idarray);
        	//rulearray.add((HashSet<Rule>) partition.getRules());
        }
        String maxTopologyPath = topologyFolder.getPath() + "/" + maxTopology;
        Topology simTopology = Util.loadFile(new TopologyFactory(new FileFactory.EndOfFileCondition(),
                new RemoveEqualIDProcessor(Util.EMPTY_LIST), new HashSet<Rule>()), maxTopologyPath, parameters, new ArrayList<Topology>(1)).get(0);
       
        List<Switch> switches = simTopology.getSwitches();
        {
        	 for (Switch aSwitch : switches) {
        		 Switchid.put(aSwitch.getId(), aSwitch);
        	 }
        }
        List<Flow> flows = Util.loadFileFilterParam(new FlowFactory(new FileFactory.EndOfFileCondition(),
                simTopology), flowFile.getPath(), parameters, new LinkedList<Flow>(), "flow\\..*");
        
        List<Flow> flows_1 = Util.loadFileFilterParam(new FlowFactory(new FileFactory.EndOfFileCondition(),
                simTopology), flowFile_1.getPath(), parameters, new LinkedList<Flow>(), "flow\\..*");
        
        final Map<Partition, Map<Rule, Collection<Flow>>> classifiedFlows = new ThreadTwoLevelTrafficProcessor(
                new OVSClassifier(),
                new OVSClassifier(), Util.threadNum).classify(flows, partitions);

        final Map<Partition, Map<Rule, Collection<Flow>>> classifiedFlows_1 = new ThreadTwoLevelTrafficProcessor(
                new OVSClassifier(),
                new OVSClassifier(), Util.threadNum).classify(flows_1, partitions);
        //prepare minimum stats
        Map<Partition, Long> minTraffic = getMinOverhead(simTopology, classifiedFlows);

        //Map<Partition, Long> minTraffic_1 = getMinOverhead(simTopology, classifiedFlows_1);
        Map<Long, Switch> vmSource = Util.loadFileFilterParam(new VMAssignmentFactory(new FileFactory.EndOfFileCondition(), simTopology), vmFile.getPath(),
                parameters, new ArrayList<Map<Long, Switch>>(), "flow\\..*").get(0);
        final Map<Switch, Collection<Partition>> sourcePartitions = NewMultiplePostPlaceScriptCluster3.getSourcePartitions(partitions, vmSource);
        //Map<Switch, Collection<Partition>> sourcePartitions = getSourcePartitions(classifiedFlows);
        
        for (Map.Entry<Switch, Collection<Partition>> entry : sourcePartitions.entrySet()) { 
        	       VMNumber= entry.getValue().size();
        	}
        
        Map<Partition, Map<Switch, Rule>> forwardingRules = MultiplePlacementScript.createForwardingRules(partitions, simTopology, sourcePartitions);

        simTopology.createHelpers(partitions, forwardingRules, classifiedFlows);
        simTopology.setRuleFlowMap(classifiedFlows);
        simTopology.setRuleFlowMap_1(classifiedFlows_1);
        
        //初始化设备搜寻的顺序
        final List<Switch> edges = simTopology.findEdges();
        
        
        //Seachswitch = FindlinkedSwitch(edges);
        for(k=0; k<edges.size(); k++) {
        	//0跳的对象
        	LinkedList<Switch> pathSwitch = new LinkedList<Switch>();
        	edges.get(k);
        	pathSwitch.add(edges.get(k));
        	HashSet<Switch> Agg1switch = new HashSet<Switch>();
        	//System.out.println(edges.get(k));
        	
        	
        	Agg1switch=FindlinkedSwitch(edges.get(k));
        	pathSwitch.addAll(Agg1switch);
        	//1跳的对象
        	HashSet<Switch> Agg2switch = new HashSet<Switch>();
        	
        	//2跳的对象
        	for(Iterator<Switch> it=Agg1switch.iterator();it.hasNext();)
        	  {
        	   //System.out.println(it.next());
        		Agg2switch.addAll(FindlinkedSwitch((Switch) it.next()));
        		//删掉0跳的对象
        	  }
        	Agg2switch.remove(edges.get(k));
        	pathSwitch.addAll(Agg2switch);
        	//3跳的对象
        	HashSet<Switch> Agg3switch = new HashSet<Switch>();
        	for(Iterator<Switch> it=Agg2switch.iterator();it.hasNext();)
        	 {
        		Agg3switch.addAll(FindlinkedSwitch((Switch) it.next()));
        	  
        	 }
        	Agg3switch.removeAll(pathSwitch);
        	pathSwitch.addAll(Agg3switch);
        	
        	//4跳的对象
        	HashSet<Switch> Agg4switch = new HashSet<Switch>();
        	for(Iterator<Switch> it=Agg3switch.iterator();it.hasNext();)
        	 {
        		Agg4switch.addAll(FindlinkedSwitch((Switch) it.next()));
        	  
        	 }
        	Agg4switch.removeAll(pathSwitch);
        	pathSwitch.addAll(Agg4switch);
        	
        	//5跳的对象
        	HashSet<Switch> Agg5switch = new HashSet<Switch>();
        	for(Iterator<Switch> it=Agg4switch.iterator();it.hasNext();)
        	 {
        		Agg5switch.addAll(FindlinkedSwitch((Switch) it.next()));
        	  
        	 }
        	Agg5switch.removeAll(pathSwitch);
        	pathSwitch.addAll(Agg5switch);
        	
        	//6跳的对象
        	HashSet<Switch> Agg6switch = new HashSet<Switch>();
        	for(Iterator<Switch> it=Agg5switch.iterator();it.hasNext();)
        	 {
        		Agg6switch.addAll(FindlinkedSwitch((Switch) it.next()));
        	  
        	 }
        	Agg6switch.removeAll(pathSwitch);
        	pathSwitch.addAll(Agg6switch);
        	
        	//Agg2switch=FindlinkedSwitch(edges.get(k));
        	findpath.put(edges.get(k),pathSwitch);
        }
//        backupRuleFlowMap = new HashMap<>(classifiedFlows.size());
//        for (Map.Entry<Partition, Map<Rule, Collection<Flow>>> entry : classifiedFlows.entrySet()) {
//            Map<Rule, Collection<Flow>> newEntry = new HashMap<>(entry.getValue().size());
//            for (Map.Entry<Rule, Collection<Flow>> entry2 : entry.getValue().entrySet()) {
//                Collection<Flow> newFlows = new ArrayList<>(entry2.getValue());
//                for (Flow flow : entry2.getValue()) {
//                    newFlows.add(flow.duplicate());
//                }
//                newEntry.put(entry2.getKey(), newFlows);
//            }
//            backupRuleFlowMap.put(entry.getKey(), newEntry);
//        }
//
        MatrixRuleSet.resetRuleSet();
        MultiplePlacementScript.resetToMatrixRuleSet(partitionFactory, partitions, simTopology, forwardingRules);

        //MemorySwitchHelper.ruleSetPool = null;
        //create objects
//        ClusterSelection partitionSelection = new MaxRuleSizeClusterSelection();
//        SwitchSelectionCluster switchSelection = new RandomSwitchSelectionCluster(Util.random);
//        Placer placer = new AssignerCluster(switchSelection, partitionSelection, 100, false, forwardingRules, null, sourcePartitions);
//        parameters.put("placement." + placer + ".partitionSelection", partitionSelection);
//        parameters.put("placement." + placer + ".switchSelection", switchSelection);

        Placer placer = new AssignmentFactory.LoadPlacer(false, forwardingRules,assignmentFolder,
//                "input\\nsdi\\classbenchassignment\\imcdiffvmfixed\\original\\-3\\2\\memory\\assignment",

                //"output\\nsdi\\classbench\\imcdiffvmfixed\\fmigrate\\original\\-3\\memory\\2_det1\\2_postplacement",
                //"output\\nsdi\\classbench\\imcdiffvmfixed\\fmigrate\\original6\\-3\\memory\\2_greedynewalphat\\0_postplacement",
                //"output\\nsdi\\classbench\\imcdiffvmfixed\\fmigrate\\original6\\-3\\memory\\2_greedymore\\2_postplacement",
                parameters, partitions, sourcePartitions);
        parameters.put("placement.alg", placer);

        ////////////////////////////////////////////////////////////////////////////////////

        final PostPlacer vmMigrator = new VMStartMigrateRandomProcessor(simTopology, Util.random, Util.threadNum, classifiedFlows,
                minTraffic, sourcePartitions, forwardingRules, 1);
        parameters.put("postPlacement0.alg", vmMigrator);

        ////////////////////////////////////////////////////////////////////////////////////

//        ThreadMinTrafficSwitchReplicateSelectionCluster postPlacerSwitchSelection = new ThreadMinTrafficSwitchReplicateSelectionCluster(Util.threadNum);
//        PostPlacer postPlacer = new ReplicateCluster(placer.getForwardingRules(),
//                replicateSwitchSelection, simTopology, false, classifiedFlows, clusters, minTraffic, partitionSources);

//        TrafficAwareSwitchSelection postPlacerSwitchSelection = new ThreadMinTrafficSameRuleSwitchSelection(Util.threadNum);
//        PostPlacer postPlacer = new DeterministicMigrateVMStartPartitions(
//                postPlacerSwitchSelection, simTopology, false, classifiedFlows, minTraffic,  sourcePartitions, forwardingRules);


//        debugPartitionMap=new HashMap<Long, Partition>();
//        for (Partition partition : partitions) {
//            debugPartitionMap.put(partition.getProperty(Util.getDimensionInfoIndex(Util.SRC_IP_INFO)).getStart(),partition);
//        }

//        final int betaSwitchSelection = 10;
////        final int betaPartitionSelection = 0;
//        final double chanceRatio = 0;
//        final RandomMinTrafficSwitchSelection postPlacerSwitchSelection = new RandomMinTrafficSwitchSelection(Util.random,
//                Util.threadNum, betaSwitchSelection, classifiedFlows);
////        PostPlacer postPlacer = new RealRandomMigrateVMStartPartitions(
////                simTopology,minTraffic,sourcePartitions,forwardingRules,Util.random,Util.threadNum,100,1,betaSwitchSelection,
////                1,1
////        );
//        PostPlacer postPlacer = new RMigrateVMStartPartition2(postPlacerSwitchSelection, simTopology, minTraffic, sourcePartitions, forwardingRules, Util.random,
//                100, 1, 0, chanceRatio, false);
//        parameters.put("postPlacement1." + postPlacer + "." + postPlacerSwitchSelection + ".beta", betaSwitchSelection);
//        //parameters.put("postPlacement1." + postPlacer + ".beta", betaPartitionSelection);
//        parameters.put("postPlacement1." + postPlacer + ".chanceRatio", chanceRatio);
//
//
//        parameters.put("postPlacement1.alg", postPlacer);
//        parameters.put("postPlacement1." + postPlacer + ".switchSelection", postPlacerSwitchSelection);

        ////////////////////////////////////////////////////////////////////////////////////

        final int initDownSteps = 1;
        final double alphaDownSteps = 0.5;
        int timeBudget = 1000000;
        PostPlacer postPlacer = new RMigrateVMStartPartition4(
                simTopology, minTraffic, sourcePartitions, forwardingRules, Util.threadNum,
                100, 1, initDownSteps, alphaDownSteps, timeBudget);
        parameters.put("postPlacement1." + postPlacer + ".initDownSteps", initDownSteps);
        parameters.put("postPlacement1." + postPlacer + ".alphaDownSteps", alphaDownSteps);
        parameters.put("postPlacement1." + postPlacer + ".timeBudget", timeBudget);

        parameters.put("postPlacement1.alg", postPlacer);

        ////////////////////////////////////////////////////////////////////////////////////

        TrafficAwareSwitchSelection postPlacerSwitchSelection2 = new ThreadMinTrafficSwitchSelection(Util.threadNum);
        PostPlacer postPlacer2 = new DeterministicMigrateVMStartPartitions3(
                postPlacerSwitchSelection2, simTopology, minTraffic, sourcePartitions, forwardingRules);


        parameters.put("postPlacement2.alg", postPlacer2);
        parameters.put("postPlacement2." + postPlacer2 + ".switchSelection", postPlacerSwitchSelection2);


        ////////////////////////////////////////////////////////////////////////////////////
        List<PostPlacer> postPlacers = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            postPlacers.add(vmMigrator);
            postPlacers.add(postPlacer);
        }


        runForTopologiesN(maxTopology, topologyFolder, outputFolder, parameters, simTopology, flows, placer, clusterFolder,
                sourcePartitions, partitions, forwardingRules, Arrays.asList(postPlacer));
    	//}

    }

    private static HashSet<Switch> FindlinkedSwitch(Switch edges) {
	// TODO Auto-generated method stub
    	//HashSet<Switch> switchset = new HashSet<Switch>();
    	//for(int k=0; k<edges.size(); k++) {
        	
        	//edges.get(k);
        	
        	//switchset.add(edges.get(k));
        	List<Link> links =edges.getLinks();
        	HashSet<Switch> Agg1 = new HashSet<Switch>();
        	for(int t=0;t<links.size();t++) {
        		Agg1.add(links.get(t).getEnd());
        	}
        
        	return Agg1;
    }


	public static Map<Switch, Collection<Partition>> getSourcePartitions(Map<Partition, Map<Rule, Collection<Flow>>> classifiedFlows) {
        Map<Switch, Collection<Partition>> output = new HashMap<Switch, Collection<Partition>>();
        for (Map.Entry<Partition, Map<Rule, Collection<Flow>>> entry : classifiedFlows.entrySet()) {
            for (Collection<Flow> flows : entry.getValue().values()) {
                if (flows.size() > 0) {
                    Flow flow = flows.iterator().next();
                    Switch source = flow.getSource();
                    Collection<Partition> partitions = output.get(source);
                    if (partitions == null) {
                        partitions = new HashSet<Partition>();
                        output.put(source, partitions);
                    }
                    partitions.add(entry.getKey());
                    //break; //break because VMSTART PARTITIONS
                }
            }
        }
        return output;
    }

    public static Map<Switch, Collection<Partition>> getSourcePartitions(List<Partition> partitions, Map<Long, Switch> switchSourceIps) {
        return new SourcePartitionFinder().process(partitions, switchSourceIps);
    }

    public static <T> void runForTopologiesN(String maxTopology, File topologyFolder, String outputFolder, Map<String, Object> parameters,
                                         Topology simTopology, List<Flow> flows,
                                         Placer placer, File clusterFolder,
                                         Map<Switch, Collection<Partition>> sourcePartitions,
                                         List<Partition> partitions, Map<Partition, Map<Switch, Rule>> forwardingRules,
                                         List<PostPlacer> postplacers) throws Exception {

      	String parentFolder = "d://vCRIB-master//vCRIB-master//examples";
        //int randomSeedIndex = Integer.parseInt(args[0]);
    	//这个randomSeedindex的具体含义还是不清楚
    	//String topologyFolderPath = parentFolder + "//topology";
        boolean successful = false;
        runNumber = 1;
        {
            System.out.println(runNumber + ": " + maxTopology);
            new File(outputFolder + "/placement").mkdirs();
            if (placer instanceof AssignerCluster) {
                List<Cluster> clusters = AssignerCluster.loadClusters(parameters, clusterFolder, partitions);
                ((AssignerCluster) placer).setClusters(clusters);
            }
            simTopology.setNumber(runNumber);
            successful = runForTopologyN(simTopology, parameters, true, flows, outputFolder, placer,
                    outputFolder + "/placement",
                    sourcePartitions, forwardingRules, postplacers, maxTopology);
            
            //File srcFile = new File(topologyFolderPath+"\\tree_degree4-1048576_4_4.txt");  
           // File dstFile = new File(topologyFolderPath+"\\tree_degree4-1048576_4_4_"+runNumber+".txt");
            //File dst1File = new File("C:\\vCRIB-master\\vCRIB-master\\examples\\topology\\tree_degree4-1048576_4_4_3.txt");
            
           // copyFile(srcFile,dstFile);
            //copyFile(srcFile,dst1File);
            //System.out.println("test-- ");
         
        }        runNumber++;
        if (successful) {
        	
        	//long ProcessStart = System.currentTimeMillis();
            
        	//while(true){
        		
        	long ProcessEnd = System.currentTimeMillis();	
        	System.out.println("endTime"+ProcessEnd);
          	//String parentFolder = "d://vCRIB-master//vCRIB-master//examples";
            //int randomSeedIndex = Integer.parseInt(args[0]);
        	//这个randomSeedindex的具体含义还是不清楚
        	//String topologyFolderPath = parentFolder + "//topology";
            //Util.threadNum = Integer.parseInt(args[0]);
        	Util.threadNum = 1;
            //这个randomseedindex是什么意思？
            //int randomSeedIndex = Integer.parseInt(args[1]);
            int randomSeedIndex = 0;
            //String maxTopology = args[2];
            //File topologyFolder = new File(args[3]);
           // String maxTopology = "21.txt";
            //topology的文件地址//
            //File topologyFolder = new File(args[3]);
           // File topologyFolder = new File(topologyFolderPath);
            //String flowFilePath = parentFolder + "//imcflowspecs0.5//1.txt";
           // String flowFilePath_1 = parentFolder + "//imcflowspecs0.5//1.txt";
            //File flowFile = new File(args[4]);
            // File flowFile_1 = new File(flowFilePath_1);
            //String inputvmFolder = parentFolder + "//inputvmFolder0.5//classbench_20_1.txt";
            //File vmFile = new File(inputvmFolder);
            String PartitonFolder = parentFolder + "//partitions//classbench_20_1.txt";
            File partitionFile = new File(PartitonFolder);
           // String outputFolder = parentFolder+ "//1//3";
            //String assignmentFolder=parentFolder+ "//1//assignment";
            //File clusterFolder = new File(args[9]);
            //File clusterFolder = new File(parentFolder+ "//1//runflows.txt");


            //Map<String, Object> parameters = new HashMap<String, Object>();
            //run(flowsFile, partitionFile, topologyFolder, parameters, maxTopology, randomSeedIndex, outputFolder);
            //laundry stuff
            Util.setRandom(randomSeedIndex);
            Util.logger.setLevel(Level.WARNING);
            {
                File outputFolderFile = new File(outputFolder);
                outputFolderFile.mkdirs();
            }


            //load data
            final UnifiedPartitionFactory partitionFactory = new UnifiedPartitionFactory(new FileFactory.EndOfFileCondition(), true, new HashSet<Rule>());
            //final UnifiedPartitionFactory partitionFactory = new UnifiedPartitionFactory(new FileFactory.EndOfFileCondition(), new HashSet<Rule>());
            List<Partition> partitions_1 = Util.loadFileFilterParam(partitionFactory, partitionFile.getPath(), parameters, new LinkedList<Partition>(), "(partition|rule)\\..*");
                 	
        	List<File> topologyFiles = new ArrayList<File>(Arrays.asList(topologyFolder.listFiles(Util.TXT_FILTER)));
            Collections.sort(topologyFiles);
            topologyFiles = SortFile(topologyFiles);
            //Map<Partition, Collection<Rule>> MapPartitionrules = new HashMap<Partition, Collection<Rule>>();
           // int k=0;
            

            for (File topologyFile : topologyFiles) {
            	//-----------------------
            	//runNumber=runNumber+8;
            	NowpostPlaceResult = Readfile("c://b.txt");
            	//final String topologyFileName_1 = topologyFile.getName();
                //String parentFolder = "d://vCRIB-master//vCRIB-master//examples";
                //String flowFilePath_2 = parentFolder + "//imcflowspecs0.5//classbench_20_"+(runNumber-2)+".txt";
            	String flowFilePath_2 = parentFolder + "//imcflowspecs0.5//"+(runNumber-2)+".txt";
                File flowFile_2 = new File(flowFilePath_2);
                List<Flow> flows_2 = Util.loadFileFilterParam(new FlowFactory(new FileFactory.EndOfFileCondition(),
                        simTopology), flowFile_2.getPath(), parameters, new LinkedList<Flow>(), "flow\\..*");
                final Map<Partition, Map<Rule, Collection<Flow>>> classifiedFlows_0 = new ThreadTwoLevelTrafficProcessor(
                        new OVSClassifier(),
                        new OVSClassifier(), Util.threadNum).classify(flows_2, partitions);
            	
            	//-----------------------
                final String topologyFileName = topologyFile.getName();
                //String parentFolder = "d://vCRIB-master//vCRIB-master//examples";
                //String flowFilePath_1 = parentFolder + "//imcflowspecs0.5//classbench_20_"+(runNumber-1)+".txt";
                String flowFilePath_1 = parentFolder + "//imcflowspecs0.5//"+(runNumber-1)+".txt"; 
                File flowFile_1 = new File(flowFilePath_1);
                List<Flow> flows_1 = Util.loadFileFilterParam(new FlowFactory(new FileFactory.EndOfFileCondition(),
                        simTopology), flowFile_1.getPath(), parameters, new LinkedList<Flow>(), "flow\\..*");
                final Map<Partition, Map<Rule, Collection<Flow>>> classifiedFlows_1 = new ThreadTwoLevelTrafficProcessor(
                        new OVSClassifier(),
                        new OVSClassifier(), Util.threadNum).classify(flows_1, partitions);
                if (topologyFileName.equals(maxTopology)) {
                    continue;
                }
                String maxTopologyPath = topologyFolder.getPath() + "/" + maxTopology;
                System.out.println(runNumber + ": " + topologyFileName);
                Topology Topology = Util.loadFile(new TopologyFactory(new FileFactory.EndOfFileCondition(),
                        new RemoveEqualIDProcessor(Util.EMPTY_LIST), new HashSet<Rule>()), maxTopologyPath, parameters, new ArrayList<Topology>(1)).get(0);
                //FileFactory<T> fileFactory  = null;
                //get now assignment
                
                
                Switch Migrateswitch = null;
                Map<Partition, Long> Increasemaps = new HashMap<>();
                Map<Partition, Integer> mark = new HashMap<>();
                Map<Partition, Switch> finalPlaceResult = new HashMap<Partition, Switch>();
                List<Switch> edges = Topology.findEdges();
                for (Partition partition : partitions) {
                	Map<Switch, Switch> switchmap =NowpostPlaceResult.get(partition);
                	for (Entry<Switch, Switch> entry : switchmap.entrySet()) { 
                		Migrateswitch = entry.getValue();              		
                	}	
                	long NewTraffic=0;
                	long OldTraffic=0;
                	long IncreaseTraffic=0;
                	if(!edges.contains(Migrateswitch)) {
                		NewTraffic = Topology.NewGetTraffic(partition, Migrateswitch, Topology,partitions,classifiedFlows_1);
                    	OldTraffic = Topology.NewGetTraffic(partition, Migrateswitch, Topology,partitions,classifiedFlows_0);
                    	IncreaseTraffic = NewTraffic-OldTraffic;
                	}else {
                		 NewTraffic = Topology.NewGetedgeTraffic(partition, Migrateswitch, Topology,partitions,classifiedFlows_1);
                         OldTraffic = Topology.NewGetedgeTraffic(partition, Migrateswitch, Topology,partitions,classifiedFlows_0);
                         IncreaseTraffic = NewTraffic-OldTraffic;
                     	
                	}
                    if(IncreaseTraffic>=0) {
                    	mark.put(partition,1);
                    }else {
                    	mark.put(partition,0);
                    	IncreaseTraffic = - IncreaseTraffic;
                    }
                    Increasemaps.put(partition, IncreaseTraffic);
                    finalPlaceResult.put(partition, Migrateswitch);
                }
                //预处理阶段
          
                Map<Switch, Set<Partition>> rplacement = generateRPlacement(finalPlaceResult);
                //placementpartition=rplacement;
                for (Switch aSwitch : Topology.getSwitches()) {
                	Set<Partition> PartitionArray = rplacement.get(aSwitch);
                	if(PartitionArray!=null) {
                	//HashSet<Rule> rulearray = new HashSet<Rule>();
                	HashSet<Integer> rulearrayid = new HashSet<Integer>();
                	Iterator<Partition> it = PartitionArray.iterator();
                	/*while(it.hasNext()) {
                		Partition tempPartition = it.next();
                		rulearray.addAll(tempPartition.getRules());
                	}*/
                	while(it.hasNext()) {
                		Partition tempPartition = it.next();
                		for(Rule ruleid:tempPartition.getRules())
                		{
                		     rulearrayid.add(ruleid.getId());
                		}
                	}
                	//TempRules.put(aSwitch,rulearray);
                	TempRulesid.put(aSwitch,rulearrayid);
                	}
                	if(edges.contains(aSwitch))
                	RedirectionruleNumber.put(aSwitch, VMNumber);
                }
                
                //System.out.println("排序前------------->" + Increasemaps);               
                Increasemaps = sortDescend(Increasemaps);// 降序排序
                //System.out.println("降序后------------->" + Increasemaps);
                //Increasemaps = sortAscend(Increasemaps);// 升序排序
                if(debug)
                System.out.println("升序后------------->" + Increasemaps);
                
                Map<Partition, Switch> SourceSwitch = new HashMap<>();
        	    for(Entry<Switch, Collection<Partition>> entry_1 : sourcePartitions.entrySet()) {
        	    	Collection<Partition> Sourcepartition = entry_1.getValue();
        	    	
        	    	 Iterator<Partition> it = Sourcepartition.iterator();
        	    	    while (it.hasNext()) {
        	    	    SourceSwitch.put((Partition) it.next(),entry_1.getKey());
        	    	    }
        	    }
                
                for (Entry<Partition, Switch> entry : finalPlaceResult.entrySet()) { 
                	Partition pop_partition = entry.getKey();
                	if(SourceSwitch.get(pop_partition).equals(entry.getValue())) {
                		int redirectionrulenumber = RedirectionruleNumber.get(SourceSwitch.get(pop_partition));
                		redirectionrulenumber = redirectionrulenumber-1;
                		RedirectionruleNumber.put(SourceSwitch.get(pop_partition), redirectionrulenumber);
                	}
                 }
                
                PlaceResult = finalPlaceResult;
                long DynamicTrafficSum_1 = 0;
                long lasttime = 0;
	            //long OldDynamicTrafficSum_1 = 0;
	            for (Partition partition : partitions) {
	               
							long DynamicTraffic_1 = Topology.NewgetTrafficForHosting(partition, PlaceResult.get(partition),Topology,classifiedFlows_1);
							//long OldDynamicTraffic = topology.OldgetTrafficForHosting(partition, PlaceforPartition.get(entry.getKey()).get(partition),topology,partitions);
							DynamicTrafficSum_1 = DynamicTrafficSum_1 + DynamicTraffic_1;
							//OldDynamicTrafficSum=OldDynamicTrafficSum+OldDynamicTraffic;
	            }
                //NowpostPlaceResult.get(key);
                long minTraffic_2 = getMinOverhead_1(Topology, classifiedFlows_1);
              	//long OldminTraffic = getMinOverhead(topology, topology.ruleFlowMap);
				long trafficoverhead_1 = DynamicTrafficSum_1 - minTraffic_2;
				//long Oldtrafficoverhead = OldDynamicTrafficSum - OldminTraffic;
				System.out.println("放置前流量开销为:"+trafficoverhead_1);
				
                
               // LinkedList<String> migratescheme = new LinkedList<String>();      
                
                for (Entry<Partition, Long> entry : Increasemaps.entrySet()) { 
                	rplacement = generateRPlacement(PlaceResult);
                	Switch Migrateswitch_1= null;
                	Partition pop_partition = entry.getKey();
                	Map<Switch, Switch> switchmap =NowpostPlaceResult.get(pop_partition);//得到当前partition所在的switch
                	for (Entry<Switch, Switch> entry_1 : switchmap.entrySet()) { 
                		Migrateswitch_1 = entry_1.getValue();              		
                	}	
                	
                	//收益函数
                	//Map<Switch, Set<Partition>> rplacement = generateRPlacement(finalPlaceResult);
                	migrationStart = System.currentTimeMillis();
                	Benfitfunction(Topology,pop_partition,Migrateswitch_1,mark,partitions,PlaceResult,classifiedFlows_1,Increasemaps,partitions_1,rplacement);
                	if(debug)
                	System.out.println(" 决策时间:" + (System.currentTimeMillis() - migrationStart));
                	lasttime = lasttime+System.currentTimeMillis() - migrationStart;
                	//更新状态
                	   long DynamicTrafficSum = 0;
		               //long OldDynamicTrafficSum = 0;
		               for (Partition partition : partitions) {
		               
								long DynamicTraffic = Topology.NewgetTrafficForHosting(partition, PlaceResult.get(partition),Topology,classifiedFlows_1);
								//long OldDynamicTraffic = topology.OldgetTrafficForHosting(partition, PlaceforPartition.get(entry.getKey()).get(partition),topology,partitions);
								DynamicTrafficSum = DynamicTrafficSum + DynamicTraffic;
								//OldDynamicTrafficSum=OldDynamicTrafficSum+OldDynamicTraffic;
		                }
		              	long minTraffic = getMinOverhead_1(Topology, classifiedFlows_1);
		              	//long OldminTraffic = getMinOverhead(topology, topology.ruleFlowMap);
						long trafficoverhead = DynamicTrafficSum - minTraffic;
						//long Oldtrafficoverhead = OldDynamicTrafficSum - OldminTraffic;
						if(originaltraffic!=trafficoverhead)
						System.out.println("放置后的流量开销为:"+trafficoverhead);	 
						originaltraffic = trafficoverhead;
                	
                	//TempRules.clear();
                	TempRulesid.clear();
                	RedirectionruleNumber.clear();
                	Map<Switch, Set<Partition>> rplacement_1 = generateRPlacement(PlaceResult);
                     //placementpartition=rplacement;
                    /* for (Switch aSwitch : Topology.getSwitches()) {
                     	Set<Partition> PartitionArray = rplacement_1.get(aSwitch);
                     	if(PartitionArray!=null) {
                     	HashSet<Rule> rulearray = new HashSet<Rule>();
                     	Iterator<Partition> it = PartitionArray.iterator();
                     	while(it.hasNext()) {
                     		Partition tempPartition = it.next();
                     		rulearray.addAll(tempPartition.getRules());
                     	}
                     	TempRules.put(aSwitch,rulearray);
                     	}
                     	if(edges.contains(aSwitch))
                     	RedirectionruleNumber.put(aSwitch, VMNumber);
                     }*/
                     
                     for (Switch aSwitch : Topology.getSwitches()) {
                     	Set<Partition> PartitionArray = rplacement_1.get(aSwitch);
                     	if(PartitionArray!=null) {
                     	//HashSet<Rule> rulearray = new HashSet<Rule>();
                     	HashSet<Integer> rulearrayid = new HashSet<Integer>();
                     	Iterator<Partition> it = PartitionArray.iterator();
                     	/*while(it.hasNext()) {
                     		Partition tempPartition = it.next();
                     		rulearray.addAll(tempPartition.getRules());
                     	}*/
                     	while(it.hasNext()) {
                     		Partition tempPartition = it.next();
                     		for(Rule ruleid:tempPartition.getRules())
                     		{
                     		     rulearrayid.add(ruleid.getId());
                     		}
                     	}
                     	//TempRules.put(aSwitch,rulearray);
                     	TempRulesid.put(aSwitch,rulearrayid);
                     	}
                     	if(edges.contains(aSwitch))
                     	RedirectionruleNumber.put(aSwitch, VMNumber);
                     }
                     
                     for (Entry<Partition, Switch> entry_1 : PlaceResult.entrySet()) { 
                     	//Switch Migrateswitch_2= null;
                     	Partition partition_pop = entry_1.getKey();
                     	if(SourceSwitch.get(partition_pop).equals(entry_1.getValue())) {
                     		int redirectionrulenumber = RedirectionruleNumber.get(SourceSwitch.get(partition_pop));
                     		redirectionrulenumber = redirectionrulenumber-1;
                     		RedirectionruleNumber.put(SourceSwitch.get(partition_pop), redirectionrulenumber);
                     	}
                      }
                     if(debug)
                 	 System.out.println(" sort time:" + (lasttime/1000));
                	 if((lasttime/1000)==3) {
                		 break;
                	 }
                }
                
                       //update topology
                {
                    Topology topology = Util.loadFile(new TopologyFactory(new FileFactory.EndOfFileCondition(),
                            new RemoveEqualIDProcessor(Util.EMPTY_LIST), new HashSet<Rule>()), topologyFile.getPath(),
                            parameters, new ArrayList<Topology>(1)).get(0);
                    for (Switch aSwitch : topology.getSwitches()) {
                        simTopology.getSwitchMap().get(aSwitch.getId()).fillParam(aSwitch);
                    }
                }
                
                if (placer instanceof AssignerCluster) {
                    List<Cluster> clusters = AssignerCluster.loadClusters(parameters, clusterFolder, partitions);
                    ((AssignerCluster) placer).setClusters(clusters);
                }
                simTopology.setRuleFlowMap_1(classifiedFlows_1);
                Map<Partition, Long> minTraffic_1 = getMinOverhead(simTopology, classifiedFlows_1);
                postplacers.get(0).updateDate(simTopology,
                		minTraffic_1,
                        sourcePartitions, forwardingRules,
                        Util.threadNum, 100, 1, 1, 0.5, 50);// 更新流量信息
          
                simTopology.setNumber(runNumber);
                
                //runForTopologyN(simTopology, parameters, false, flows, outputFolder, placer,
                //        outputFolder + "/placement",
                //        sourcePartitions, forwardingRules, postplacers, topologyFileName);
                Writetofile(NowpostPlaceResult);
                runNumber++;
            }
        }
        System.out.println("finished");
    }



	private static Map<Partition, Map<Switch, Switch>> Benfitfunction(Topology topology, Partition pop_partition, Switch migrateswitch_1, Map<Partition, Integer> mark, List<Partition> partitions, Map<Partition, Switch> finalPlaceResult, Map<Partition, Map<Rule, Collection<Flow>>> classifiedFlows_1, Map<Partition, Long> increasemaps, List<Partition> partitions_1,Map<Switch, Set<Partition>> rplacement) {
		// TODO Auto-generated method stub
		//System.out.println(Maprule.size());
		//int t=Maprule.size();
		String parentFolder = "d://vCRIB-master//vCRIB-master//examples";
		//String topologyFolderPath = parentFolder + "//topology";
		String inputvmFolder = parentFolder + "//inputvmFolder0.5//classbench_84649_1.txt";
		Map<String, Object> parameters = new HashMap<String, Object>();
	    File vmFile = new File(inputvmFolder);
	    
		//final List<Switch> edges = topology.findEdges();
		Map<Partition, Map<Switch, Switch>> Returnplacementresult = new HashMap<Partition, Map<Switch, Switch>>();
		Map<Long, Switch> vmSource = null;
		Boolean Finddevice =false;
		try {
			vmSource = Util.loadFileFilterParam(new VMAssignmentFactory(new FileFactory.EndOfFileCondition(), topology), vmFile.getPath(),
			            parameters, new ArrayList<Map<Long, Switch>>(), "flow\\..*").get(0);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(debug)
		System.out.println("进入决策时的分区："+pop_partition);
	    Map<Switch, Collection<Partition>> sourcePartitions = NewMultiplePostPlaceScriptCluster3.getSourcePartitions(partitions, vmSource);
	    Map<Partition, Switch> SourceSwitch = new HashMap<>();
	    for(Entry<Switch, Collection<Partition>> entry_1 : sourcePartitions.entrySet()) {
	    	Collection<Partition> Sourcepartition = entry_1.getValue();
	    	
	    	 Iterator<Partition> it = Sourcepartition.iterator();
	    	    while (it.hasNext()) {
	    	    SourceSwitch.put((Partition) it.next(),entry_1.getKey());
	    	    }
	    }
	    //System.out.println();//1、如果这个分区流量开销增加了，并且在最优的放置位置上
        long procedurestart = System.currentTimeMillis();
       /*if((mark.get(pop_partition) ==1) && (migrateswitch_1.equals(SourceSwitch.get(pop_partition)))) {
	    	
	    	System.out.println("情况1");
	    	HashMap<Switch, Switch> hmap= new HashMap<Switch, Switch>();
	    	hmap.put(migrateswitch_1, migrateswitch_1);
	    	Returnplacementresult.put(pop_partition, hmap); //不需要移动位置，已经在最佳位置，且收益为正
	    	if(debug) {
	    	System.out.println("情况1：流量增加，但在最佳位置，分区："+pop_partition+"，保持在设备："+hmap+" 持续时间："+(System.currentTimeMillis()-procedurestart));
	    	System.out.println("情况1,持续时间："+(System.currentTimeMillis()-procedurestart));}
	    	
	        	
	    }else */
	    if(!migrateswitch_1.equals(SourceSwitch.get(pop_partition))) {
	    	//检查最优设备上是否能放下分区
	    	if(debug)
	    	System.out.println("情况2:分区不在最佳位置上");
        	LinkedList<Switch> pathSwitch = findpath.get(SourceSwitch.get(pop_partition));
        	
        	//1、检查最优的交换机是否能放下这个分区
        	for(int k=0; k<pathSwitch.size();k++) {
        		if(!Finddevice) {//如果没有找到设备
        		Set<Partition> NewPartition= rplacement.get(pathSwitch.get(k));  //获得位置上的分区
        		if(NewPartition!=null) {
        		HashSet<Integer> AllRulesid = new HashSet<Integer>();
        		HashSet<Integer> finalAllRulesid = new HashSet<Integer>();
        		Iterator<Partition> it = NewPartition.iterator();
        		Map<Partition, Float> Blockmark = new HashMap<>();
        		LinkedList<Integer> AllRulesArrayid = new LinkedList<Integer>();
        		int rulesize = 0;
        		while (it.hasNext()) {  
        			try {
        				Partition nowPartiton = it.next();
						long blocktraffic = topology.GetBlockTraffic(nowPartiton, topology, classifiedFlows_1);                       	
                  
                        Blockmark.put(nowPartiton,(float)blocktraffic/nowPartiton.getSize());
                        //AllRules.addAll(MapPartitionrules.get(nowPartiton));
                        rulesize = AllRulesid.size();
                        AllRulesid.addAll(MapPartitionrulesid.get(nowPartiton.getId()));
                        if(debug)
                        //System.out.println("添加分区："+nowPartiton+"，规则数量增加了："+(AllRulesid.size()-rulesize));
                        //AllRules.addAll(nowPartiton.getRules());
                        //AllRulesArray.addAll(nowPartiton.getRules());
                        //AllRulesArray.addAll(MapPartitionrules.get(nowPartiton));
                        AllRulesArrayid.addAll(MapPartitionrulesid.get(nowPartiton.getId()));
        			} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
        			
        		}
        		Blockmark = sortAscend(Blockmark);
        		if(debug)
        		System.out.println("设备："+pathSwitch.get(k)+"分区放入前，"+AllRulesid.size());
        		//AllRules.addAll(MapPartitionrules.get(pop_partition));
        		AllRulesid.addAll(MapPartitionrulesid.get(pop_partition.getId()));
        		if(debug)
        		System.out.println("添加分区："+pop_partition+"，规则数量增加了："+(AllRulesid.size()-rulesize));
        		//AllRulesArray.addAll(MapPartitionrules.get(pop_partition));
        		AllRulesArrayid.addAll(MapPartitionrulesid.get(pop_partition.getId()));
        		if(debug)
        		System.out.println("设备："+pathSwitch.get(k)+"分区在分区放入后，"+AllRulesid.size());
        		int occupyresource = AllRulesid.size();
        		MemorySwitch memorySwitch = (MemorySwitch) pathSwitch.get(k);
        		//int capacity = memorySwitch.getMemoryCapacity();
        		int capacity = memorySwitch.getMemoryCapacity();
        		int originalcapacity = capacity;
        		List<Switch> edges = topology.findEdges();
        		HashMap<Switch, Switch> hmap= new HashMap<Switch, Switch>();
    	    	if(k==0) {
    	    		capacity = capacity-RedirectionruleNumber.get(pathSwitch.get(0))+1;//如果是源地址，那么需要考虑重定向规则
    	    	}else if(edges.contains(pathSwitch.get(k))){
    	    		capacity = capacity-RedirectionruleNumber.get(pathSwitch.get(k));
    	    	}
        		if(occupyresource<=capacity) {//如果容量满足大小
        			
        			if(pathSwitch.get(k).getId().equals(migrateswitch_1.getId())) {
        				if(debug)
        				System.out.println("已经找到设备:"+pathSwitch.get(k).getId()+" 原设备也是："+migrateswitch_1.getId()+" 所以退出出计算");
        				break;
        			}
        			//System.out.println("情况2：分区可以迁移到设备上去，容量大小满足资源约束");
        			hmap.put(migrateswitch_1, pathSwitch.get(k));
        			Returnplacementresult.put(pop_partition, hmap);//如果资源满足，那么迁移过去
        			//TempRules.put(pathSwitch.get(k),AllRules);//更新当前的规则数量
        			if(k==0) {
        				int value = RedirectionruleNumber.get(pathSwitch.get(k))-1;
        				RedirectionruleNumber.put(pathSwitch.get(k), value);
        			}
        			long GetBenfit=0;
        			long oldBenfit=0;
        			long Benefit=0;
        			try {
						GetBenfit = topology.BenfitTraffic(pop_partition, pathSwitch.get(k), topology, classifiedFlows_1);
						oldBenfit = topology.BenfitTraffic(pop_partition, migrateswitch_1, topology, classifiedFlows_1);
						Benefit =  oldBenfit - GetBenfit;
        			} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}					
        			if(Benefit>0) {
        			
        			NowpostPlaceResult.put(pop_partition, hmap);//修改NowpostPlaceResult的值
        			if(debug)
        			System.out.println("情况2-1：流量增加，但不在最优位置，可移动到最佳位置,分区为："+pop_partition+":"+hmap+" 情况2移动获得的收益为:"+Benefit);
        			PlaceResult.put(pop_partition, pathSwitch.get(k)); //修改PlaceResult的值
        			}
        			//System.out.println(pop_partition+":"+pathSwitch.get(k));
        			break;
        		}else {//如果资源不满足，则试图迁移出一些分区
        			//如果资源不满足，则试图迁移出一些分区
        			//构建一个表
        			
        			Integer Distance =0;
        			if(k==0) {
        			    Distance = occupyresource-originalcapacity+RedirectionruleNumber.get(pathSwitch.get(0))-1;
        			    if(debug)
        			    System.out.println("情况2-2："+pathSwitch.get(k)+"上资源不足，试图移动一部分分区到次优位置"+" 需要挪出资源数量为："+Distance+" 重定向规则数量为："+RedirectionruleNumber.get(pathSwitch.get(0)));
        			}else {
        				Distance = occupyresource-originalcapacity;
        				if(debug)
        				System.out.println("情况2-2："+pathSwitch.get(k)+"上资源不足，试图移动一部分分区到次优位置"+" 需要挪出资源数量为："+Distance);
        			}
        			HashMap<Integer, Integer> RuleCountid = new HashMap<Integer, Integer>();       			
        			Iterator<Integer> ruleid = AllRulesid.iterator();
        			while (ruleid.hasNext()) {
        				RuleCountid.put(ruleid.next(), 0);
        			}
        	
        			for(int l=0; l<AllRulesArrayid.size(); l++) {
        				Integer newvalue= RuleCountid.get(AllRulesArrayid.get(l));
        				//if(AllRulesArray.get(l)!=null)
        				RuleCountid.put(AllRulesArrayid.get(l),newvalue+1);
        			}
        			Integer reducerule =0;
        			HashSet<Partition> migratePartitionArray = new HashSet<Partition>();
        			for (Entry<Partition, Float> entry : Blockmark.entrySet()) { 
                     	//Switch Migrateswitch_1= null;
        				if(Distance > reducerule) {      					
                     	Partition Migrate_partition = entry.getKey();
                     	migratePartitionArray.add(Migrate_partition);
                     	Collection<Integer> rulearrayid = MapPartitionrulesid.get(Migrate_partition.getId());
                     	Iterator<Integer> moveruleid = rulearrayid.iterator() ;
                     	while(moveruleid.hasNext()) {
                     		Integer migrateruleid = moveruleid.next();
                     		Integer newvalue= RuleCountid.get(migrateruleid);
                     		newvalue = newvalue-1;
                     		if(newvalue==0) {
                     			reducerule=reducerule+1;
                     		}
                     		RuleCountid.put(migrateruleid,newvalue);
                     		
                     	}
                     	//if(debug)
                     	//System.out.println("情况2-2：移出分区；"+Migrate_partition+" 减少了："+reducerule+"总共分区数量有："+Blockmark.size());
        				}
        			 }
        
        			for (Entry<Integer, Integer> entry : RuleCountid.entrySet()) {
        				if(entry.getValue()!=0) {
        					finalAllRulesid.add(entry.getKey());
        				}
        			}
        			
        			Iterator<Partition> Migrate_partition = migratePartitionArray.iterator();
        			HashMap<Partition,Switch> TargetSwitch = new HashMap<Partition,Switch>();
        			while (Migrate_partition.hasNext()) {  
        				Partition Migrate_p = Migrate_partition.next();
        				for(int p = k+1; p<pathSwitch.size();p++) {
					
        					Set<Integer> rulesid = new HashSet<Integer>();
        				    if(TempRulesid.get(pathSwitch.get(p))!=null)
        					rulesid.addAll(TempRulesid.get(pathSwitch.get(p)));
        					rulesid.addAll(MapPartitionrulesid.get(Migrate_p.getId()));
        					
        					//Integer occupy = rules.size();
        					Integer occupy = rulesid.size();
        					MemorySwitch memorySwitch_1 = (MemorySwitch) pathSwitch.get(p);
        	        		int capacity_1 = memorySwitch_1.getMemoryCapacity();
        	        	
        	    	    	if(edges.contains(pathSwitch.get(k))){
        	    	    		capacity = capacity-RedirectionruleNumber.get(pathSwitch.get(k));
        	    	    	}
        					if(occupy<=capacity_1) {
        						TargetSwitch.put(Migrate_p, pathSwitch.get(p));
        						break;
        					}
        				}
        			}
        			
        			//计算收益
        			//1、获得的收益
        			try {
						long GetBenfit = topology.BenfitTraffic(pop_partition, pathSwitch.get(k), topology, classifiedFlows_1);					
						long oldBenfit = topology.BenfitTraffic(pop_partition, migrateswitch_1, topology, classifiedFlows_1);
						long Benefit =  oldBenfit - GetBenfit;
						long lossBenfit =0;
						Iterator migratemap=TargetSwitch.entrySet().iterator();
						while(migratemap.hasNext()) {
							Map.Entry<Partition,Switch> entry=(Entry<Partition,Switch>) migratemap.next();
							long beforelossBenfit = topology.BenfitTraffic(entry.getKey(), entry.getValue(), topology, classifiedFlows_1);					
							long oldlossBenfit = topology.BenfitTraffic(entry.getKey(), pathSwitch.get(k), topology, classifiedFlows_1);
							lossBenfit=lossBenfit+beforelossBenfit-oldlossBenfit;
						}
						if(Benefit>lossBenfit) {
							hmap.put(migrateswitch_1, pathSwitch.get(k));
		        			Returnplacementresult.put(pop_partition, hmap);//如果资源满足，那么迁移过去
		        			//TempRules.put(pathSwitch.get(k),finalAllRules);//更新当前的规则数量
		        			if(k==0) {
		        				int value = RedirectionruleNumber.get(pathSwitch.get(k))-1;
		        				RedirectionruleNumber.put(pathSwitch.get(k), value);
		        			}
		        			NowpostPlaceResult.put(pop_partition, hmap);//修改NowpostPlaceResult的值
		        			System.out.println(pop_partition+":"+hmap+" get benefit:"+(Benefit-lossBenfit));
		        			PlaceResult.put(pop_partition, pathSwitch.get(k));
							Finddevice =true;
							migratemap=TargetSwitch.entrySet().iterator();
							while(migratemap.hasNext()) {
								Map.Entry<Partition,Switch> entry=(Entry<Partition,Switch>) migratemap.next();
								HashMap<Switch, Switch> hmap_1= new HashMap<Switch, Switch>();
								hmap_1.put(pathSwitch.get(k),entry.getValue());
								NowpostPlaceResult.put(entry.getKey(), hmap_1);//将迁移的分区修改值
								if(debug)
								System.out.println("情况 2-2资源不足但是通过替换分区获得收益："+entry.getKey()+":"+hmap_1+" get benefit:"+(Benefit-lossBenfit)+" 持续时间："+(System.currentTimeMillis()-procedurestart));
								//HashSet<Rule> finalRule=TempRules.get(entry.getValue());
								//finalRule.addAll(MapPartitionrules.get(entry.getKey()));
								//TempRules.put(entry.getValue(),finalAllRules);
								PlaceResult.put(entry.getKey(), entry.getValue());
							}
							break;
						}
        			} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}	
        			
        		
        		}
        	}else {
        		MemorySwitch memorySwitch = (MemorySwitch) pathSwitch.get(k);
        		HashMap<Switch, Switch> hmap= new HashMap<Switch, Switch>();
        		//int capacity = memorySwitch.getMemoryCapacity();
        		int capacity = memorySwitch.getMemoryCapacity();
        		if(pop_partition.getRules().size()<capacity-VMNumber+1) {
        			
        			long GetBenfit = 0;
        			long oldBenfit = 0;
					try {
						GetBenfit = topology.BenfitTraffic(pop_partition, pathSwitch.get(k), topology, classifiedFlows_1);
						oldBenfit = topology.BenfitTraffic(pop_partition, migrateswitch_1, topology, classifiedFlows_1);
										
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}					
					long Benefit =  oldBenfit - GetBenfit;
					if(Benefit>0) {
        	    	hmap.put(migrateswitch_1, pathSwitch.get(k));
        	    	Returnplacementresult.put(pop_partition, hmap);
        	    	//TempRules.put(pathSwitch.get(k),(HashSet<Rule>) MapPartitionrules.get(pop_partition));//更新当前的规则数量
        			if(k==0) {
        				int value = RedirectionruleNumber.get(pathSwitch.get(k))-1;
        				RedirectionruleNumber.put(pathSwitch.get(k), value);
        			}
        			NowpostPlaceResult.put(pop_partition, hmap);//修改NowpostPlaceResult的值
        			if(debug)
        			System.out.println("情况2："+pop_partition+":"+hmap+" 获得收益:"+Benefit+" 持续时间："+(System.currentTimeMillis()-procedurestart));
        			PlaceResult.put(pop_partition, pathSwitch.get(k));
        	    	Finddevice =true;
        	    	break;
					}
        	    
        		}
        		
        	}
        	}
        	}
        	if(debug)
        	System.out.println("情况2,持续时间："+(System.currentTimeMillis()-procedurestart));
	    }else if((migrateswitch_1.equals(SourceSwitch.get(pop_partition)))) {
	    	//(mark.get(pop_partition) ==0) && 
		    //HashMap<Switch, Switch> hmap= new HashMap<Switch, Switch>();
	    	//hmap.put(migrateswitch_1, migrateswitch_1);
	    	//Returnplacementresult.put(pop_partition, hmap);//不需要移动位置。保留在原位
	    	System.out.println("情况3");
	    	//如果流量减少了，并且在最优的位置上，试图迁移到其他的位置上，并检查收益
	    	LinkedList<Switch> pathSwitch = findpath.get(SourceSwitch.get(pop_partition));
	    	//Map<Switch, Set<Partition>> rplacement = generateRPlacement(finalPlaceResult);
	    	HashMap<Partition,Switch> TargetSwitch = new HashMap<Partition,Switch>();
	    	long lossBenfit =0;
	    	//HashSet<Rule> finalRules=new HashSet<Rule>();
	    	HashSet<Integer> finalRulesid=new HashSet<Integer>();
	    	for(int k=1; k<pathSwitch.size();k++) {
	    		
	    		/*Set<Rule> rules = new HashSet<Rule>(); 
	    		if(TempRules.get(pathSwitch.get(k))!=null)
	    		rules.addAll(TempRules.get(pathSwitch.get(k)));
	    		rules.addAll(pop_partition.getRules());*/
	    		
	    		Set<Integer> rulesid = new HashSet<Integer>(); 
	    		if(TempRulesid.get(pathSwitch.get(k))!=null)
	    		rulesid.addAll(TempRulesid.get(pathSwitch.get(k)));
	    		rulesid.addAll(MapPartitionrulesid.get(pop_partition.getId()));
				//Integer occupy = rules.size();
	    		Integer occupy = rulesid.size();
				MemorySwitch memorySwitch_1 = (MemorySwitch) pathSwitch.get(k);
        		int capacity_1 = memorySwitch_1.getMemoryCapacity();
				if(occupy<=capacity_1) {
					//finalRules.addAll(rules);
					finalRulesid.addAll(rulesid);
					TargetSwitch.put(pop_partition, pathSwitch.get(k));
					try {
						lossBenfit = topology.BenfitTraffic(pop_partition, TargetSwitch.get(pop_partition), topology, classifiedFlows_1);
						long originalbenfit = topology.BenfitTraffic(pop_partition, SourceSwitch.get(pop_partition), topology, classifiedFlows_1);
						lossBenfit = lossBenfit-originalbenfit;
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					break;
				}		
	    	}
	    	Switch edg_switch= SourceSwitch.get(pop_partition);
	    	Map<Partition,Long> markList = new HashMap<Partition,Long>();
	    	LinkedList<Partition> edge_PartitionArray = new LinkedList<Partition>();
	    	for (Map.Entry<Partition, Switch> entry : SourceSwitch.entrySet()) { 
	    		String id = edg_switch.getId();
				if(entry.getValue().toString().equals(id) && !entry.getKey().equals(pop_partition)) {
	    			 edge_PartitionArray.add(entry.getKey());
	    			 markList.put(entry.getKey(), increasemaps.get(entry.getKey()));
	    		 }
	    	}
	    	markList = sortDescend(markList);
	    	Set<Partition> Partitons = rplacement.get(migrateswitch_1);
	    	
	    	//Collection<Rule> rules = new HashSet<Rule>();
	    	Collection<Integer> rulesid = new HashSet<Integer>();
	    	MemorySwitch memorySwitch_1 = (MemorySwitch) SourceSwitch.get(pop_partition);
    		int capacity_1 = memorySwitch_1.getMemoryCapacity();
	    	capacity_1= capacity_1-RedirectionruleNumber.get(migrateswitch_1)-1;
    		Partitons.remove(pop_partition);
	    	Set<Partition> AddPartitons = new HashSet<Partition>();
	    	//HashSet<Rule> finalrules = new HashSet<Rule>();
	    	HashSet<Integer> finalrulesid = new HashSet<Integer>();
	    	
	    	for (Entry<Partition, Long> entry : markList.entrySet()) { 
	    		rulesid.addAll(MapPartitionrulesid.get(entry.getKey().getId()));
	    	  	for (Partition p:Partitons) {
	    	  		rulesid.addAll(MapPartitionrulesid.get(p.getId()));
		    	}
	    	  	if(capacity_1+AddPartitons.size()+1>=rulesid.size()) {
	    	  		Partitons.add(entry.getKey());
	    	  		AddPartitons.add(entry.getKey());
	    	  		finalrulesid.addAll(rulesid);
	    	  	}
	    	  	rulesid.clear();
	    	}
	    	
	    	long getBenfit=0;
	    	for(Partition p:AddPartitons) {
	    		
				try {
	    		Map<Switch, Switch> switchmap =NowpostPlaceResult.get(p);
	    		Switch CurrentSwitch_1 = null;
	        	for (Entry<Switch, Switch> entry_1 : switchmap.entrySet()) { 
	        		CurrentSwitch_1 = entry_1.getValue(); 
	        		
	        	}
	    		
					long beforegetBenfit = topology.BenfitTraffic(p, CurrentSwitch_1, topology, classifiedFlows_1);					

					long oldgetBenfit = topology.BenfitTraffic(p, SourceSwitch.get(pop_partition), topology, classifiedFlows_1);
					//getBenfit=getBenfit+oldgetBenfit-beforegetBenfit;
					
					getBenfit = getBenfit+beforegetBenfit-oldgetBenfit;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			
			    		
	    	}
	    	getBenfit=getBenfit-lossBenfit;
	    	if(getBenfit>0) {
	    		//如果收益为正，则执行迁移
	    		HashMap<Switch, Switch> hmap= new HashMap<Switch, Switch>();
	    	  	hmap.put(migrateswitch_1, TargetSwitch.get(pop_partition));
    	    	Returnplacementresult.put(pop_partition, hmap);
    	    	//TempRules.put(TargetSwitch.get(pop_partition),finalRules);//更新当前的规则数量
    			int value = RedirectionruleNumber.get(migrateswitch_1)+1-AddPartitons.size();
    			RedirectionruleNumber.put(migrateswitch_1, value);
    			NowpostPlaceResult.put(pop_partition, hmap);//修改NowpostPlaceResult的值
    			if(debug)
    			System.out.println("情况3，交换分区到:"+TargetSwitch.get(pop_partition)+"设备上"+pop_partition+":"+hmap+" get benefit:"+getBenfit+" 持续时间："+(System.currentTimeMillis()-procedurestart));
    			PlaceResult.put(pop_partition, TargetSwitch.get(pop_partition));
    			for(Partition p:AddPartitons) {

    	    		Map<Switch, Switch> switchmap =NowpostPlaceResult.get(p);
    	    		Switch CurrentSwitch_1 = null;
    	        	for (Entry<Switch, Switch> entry_1 : switchmap.entrySet()) { 
    	        		CurrentSwitch_1 = entry_1.getValue(); 
    	        		
    	        	}
    	        	HashMap<Switch, Switch> hmap_1= new HashMap<Switch, Switch>();
    	        	hmap_1.put(CurrentSwitch_1, SourceSwitch.get(pop_partition));
    	        	Returnplacementresult.put(p, hmap_1);
    	        	NowpostPlaceResult.put(p, hmap_1);
    	        	if(debug)
    	        	System.out.println("情况3,分区"+p+"通过交换:"+hmap_1+" 得到收益:"+getBenfit+" 持续时间："+(System.currentTimeMillis()-procedurestart));
    	        	PlaceResult.put(p,SourceSwitch.get(pop_partition));
    	        	
    			}
    			//TempRules.put(TargetSwitch.get(pop_partition), finalRules);
    			TempRulesid.put(TargetSwitch.get(pop_partition), finalRulesid);
    			//TempRules.put(SourceSwitch.get(pop_partition), finalRules);
    			
	    	}
	    	if(debug)
	    	System.out.println("情况3,持续时间："+(System.currentTimeMillis()-procedurestart));
	    }/*else if((mark.get(pop_partition) ==0) && (!migrateswitch_1.equals(SourceSwitch.get(pop_partition)))) {
	    	System.out.println("情况4");
	    	LinkedList<Switch> pathSwitch = findpath.get(SourceSwitch.get(pop_partition));
	    	//Map<Switch, Set<Partition>> rplacement = generateRPlacement(finalPlaceResult);
	    	Map<Switch, Set<Partition>> rplacement = generateRPlacement(SourceSwitch);
	    	//System.out.println(SourceSwitch.get(pop_partition).toString());
	    	HashSet<Partition> partitionArray = new HashSet<Partition>();
	    	partitionArray.addAll(rplacement.get(SourceSwitch.get(pop_partition)));
	    	Map<Partition,Long> markList = new HashMap<Partition,Long>();
	    	//markList.put(key, remappingFunction);
	    	String[] SwitchString = SourceSwitch.get(pop_partition).toString().split("_");
	    	for (Entry<Partition, Switch> entry : SourceSwitch.entrySet()) { //找到同一个服务器的分区
	    		Switch getSwitch = entry.getValue(); 
	    		String[] SwitchString_1=getSwitch.getId().split("_");
	    		if(SwitchString_1[0].equals(SwitchString[0])&&SwitchString_1[1].equals(SwitchString[1])&&SwitchString_1[2].equals(SwitchString[2])&&!getSwitch.getId().equals(SourceSwitch.get(pop_partition).toString()) ) {
	    			//System.out.println(getSwitch.getId());
	    			if(rplacement.get(SourceSwitch.get(pop_partition))!=null)
	    			partitionArray.addAll(rplacement.get(SourceSwitch.get(pop_partition)));
	    		}
        		
        	}
	    	
	    	HashMap<Partition,Switch> TargetSwitch = new HashMap<Partition,Switch>();
	    	long getBenfit=0;
	    	long Totalbenfit=0;
	    	Boolean CangetBeneift =true;
	    	for(int k=0; k<pathSwitch.size();k++) {
	    	
	    		/*Set<Rule> rules = new HashSet<Rule>();
	    		if(TempRules.get(pathSwitch.get(k))!=null) {
	    	    rules.addAll(TempRules.get(pathSwitch.get(k)));
	    		}
	    	    rules.addAll(MapPartitionrules.get(pop_partition));
	    	    */
	    	/*    Set<Integer> rulesid = new HashSet<Integer>();
	    		if(TempRulesid.get(pathSwitch.get(k))!=null) {
	    	    rulesid.addAll(TempRulesid.get(pathSwitch.get(k)));
	    		}
	    		System.out.println("当前设备："+pathSwitch.get(k)+" 当前的设备的资源占用为："+rulesid.size());
	    	    rulesid.addAll(MapPartitionrulesid.get(pop_partition.getId()));
	    	    System.out.println("加入分区后，规则数量增加为："+rulesid.size());
	    	    //System.out.println(partitions_1.get(Partitionchid_1.get(pop_partition)).getRules());
				Integer occupy = rulesid.size();
				MemorySwitch memorySwitch_1 = (MemorySwitch) pathSwitch.get(k);
				 List<Switch> edges = topology.findEdges();
        		int capacity_1 = memorySwitch_1.getMemoryCapacity();
        		if(k==0) {
    	    		capacity_1 = capacity_1-RedirectionruleNumber.get(pathSwitch.get(0))+1;//如果是源地址，那么需要考虑重定向规则
    	    	}else if(edges.contains(pathSwitch.get(k))){
    	    		capacity_1 = capacity_1-RedirectionruleNumber.get(pathSwitch.get(k));
    	    	}
        		System.out.println("移动到设备："+pathSwitch.get(k)+"占用的空间为："+occupy+" 而空间大小为："+capacity_1);	
				if(occupy<=capacity_1) {
					TargetSwitch.put(pop_partition, pathSwitch.get(k));
					//TempRules.put(pathSwitch.get(k), (HashSet<Rule>) rules);
					TempRulesid.put(pathSwitch.get(k), (HashSet<Integer>) rulesid);
					Map<Switch, Switch> switchmap =NowpostPlaceResult.get(pop_partition);
		    		Switch CurrentSwitch_1 = null;
		        	for (Entry<Switch, Switch> entry_1 : switchmap.entrySet()) { 
		        		CurrentSwitch_1 = entry_1.getValue(); 
		        		
		        	}
		        	
		        	if(pathSwitch.get(k).getId().equals(CurrentSwitch_1.getId())) {
		        		CangetBeneift =false;
		    			break;
		    		}
					
					try {
						long beforegetBenfit = topology.BenfitTraffic(pop_partition, pathSwitch.get(k), topology, classifiedFlows_1);
					
						long oldgetBenfit = topology.BenfitTraffic(pop_partition, CurrentSwitch_1, topology, classifiedFlows_1);
						
						getBenfit=oldgetBenfit-beforegetBenfit;
						if(getBenfit>0) {
							Totalbenfit = getBenfit;
							HashMap<Switch, Switch> hmap= new HashMap<Switch, Switch>();
							hmap.put(CurrentSwitch_1, pathSwitch.get(k));
		        	    	Returnplacementresult.put(pop_partition, hmap);
		        	    	//TempRules.put(pathSwitch.get(k),(HashSet<Rule>) rules);//更新当前的规则数量
		        			if(k==0) {
		        				int value = RedirectionruleNumber.get(pathSwitch.get(k))-1;
		        				RedirectionruleNumber.put(pathSwitch.get(k), value);
		        			}
		        			NowpostPlaceResult.put(pop_partition, hmap);//修改NowpostPlaceResult的值
		        			System.out.println("情况4，分区："+pop_partition+" 通过交换:"+hmap+" 获得收益:"+getBenfit);
		        			PlaceResult.put(pop_partition,pathSwitch.get(k));
		        			finalPlaceResult.put(pop_partition, pathSwitch.get(k));
		        			TempRulesid.put(CurrentSwitch_1,(HashSet<Integer>) rulesid);
							CangetBeneift = true;
							break;
						}
					
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}					

				
			
				}		
				
	    	}
	    	if(CangetBeneift) {
	    	Map<Switch, Switch> switchmap =NowpostPlaceResult.get(pop_partition);
    		Switch CurrentSwitch_1 = null;
    		
        	for (Entry<Switch, Switch> entry_1 : switchmap.entrySet()) { 
        		CurrentSwitch_1 = entry_1.getKey(); 
        		
        	}
        	Set<Partition> AddPartitons = new HashSet<Partition>() ;
	    	MemorySwitch memorySwitch_1 = (MemorySwitch) CurrentSwitch_1;
    		int capacity_1 = memorySwitch_1.getMemoryCapacity();
	    	//Collection<Rule> rules = new HashSet<Rule>();
	    	Collection<Integer> rulesid = new HashSet<Integer>();
	    	partitionArray.remove(pop_partition);
	    	for(Partition p:partitionArray) {
	    		
	    		markList.put(p, increasemaps.get(p));
	    	}
	    	markList = sortDescend(markList);
	    	//Collection<Rule> rules = new HashSet<Rule>();
	    	Map<Switch, Set<Partition>> rplacement_1 = generateRPlacement(finalPlaceResult);
	    	Set<Partition> Partitons = rplacement_1.get(CurrentSwitch_1);
	    	long Getbenfit=0;
	    	//Collection<Rule> rules = new HashSet<Rule>();
	    	if(Partitons!=null) {
	    	Partitons.remove(pop_partition);
	    	for (Entry<Partition, Long> entry : markList.entrySet()) { 
	    		rulesid.addAll(TempRulesid.get(CurrentSwitch_1));
	    	  	for (Partition p:Partitons) {
	    	  		rulesid.addAll(MapPartitionrulesid.get(p.getId()));
		    	}
	    	  	if(capacity_1>rulesid.size()) {//不是源地址设备，所以容量不需要考虑重定向规则
	    	  				long oldgetBenfit;
	    	  				long beforegetBenfit;
					try {
						beforegetBenfit = topology.BenfitTraffic(entry.getKey(), CurrentSwitch_1, topology, classifiedFlows_1);											
						oldgetBenfit = topology.BenfitTraffic(entry.getKey(), finalPlaceResult.get(entry.getKey()), topology, classifiedFlows_1);
						//getBenfit=getBenfit+oldgetBenfit-beforegetBenfit;
						Getbenfit = oldgetBenfit-beforegetBenfit;
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
	    	  		if(Getbenfit>0) {
	    	  	    Totalbenfit = Totalbenfit+Getbenfit;
	    			HashMap<Switch, Switch> hmap= new HashMap<Switch, Switch>();
					hmap.put(finalPlaceResult.get(entry.getKey()), CurrentSwitch_1);
        	    	Returnplacementresult.put(entry.getKey(), hmap);
        	    	//TempRules.put(CurrentSwitch_1,(HashSet<Rule>) rules);//更新当前的规则数量
        			//if(k==0) {
        			//	int value = RedirectionruleNumber.get(pathSwitch.get(k))-1;
        			//	RedirectionruleNumber.put(pathSwitch.get(k), value);
        			//}
        			NowpostPlaceResult.put(entry.getKey(), hmap);//修改NowpostPlaceResult的值
        			System.out.println("part 4"+entry.getKey()+":"+hmap+" get benefit:"+Getbenfit);
        			PlaceResult.put(entry.getKey(),CurrentSwitch_1);
	    	  		Partitons.add(entry.getKey());
	    	  		TempRulesid.put(CurrentSwitch_1, (HashSet<Integer>) rulesid);
	    	  		AddPartitons.add(entry.getKey());
	    	  		}
	    	  	}
	    	  	rulesid.clear();
	    	}
	    	if(Totalbenfit>0) {
	    		
	    	}
              
	    	}
	    }
	    }*/
	    	return Returnplacementresult;
	  
	}

	public static boolean runForTopologyN(Topology topology,
                                           Map<String, Object> parameters, boolean firstRun, List<Flow> flows,
                                           String outputFolder, Placer placer,
                                           String placementOutputFolder,
                                           Map<Switch, Collection<Partition>> sourcePartitions,
                                           Map<Partition, Map<Switch, Rule>> forwardingRules,
                                           List<PostPlacer> postPlacers,
                                           String topologyName) throws Exception {
        boolean successful = false;
        Assignment assignment = null;
        try {
        	//if(runNumber==1) {
            assignment = placer.place(topology, forwardingRules.keySet());//获得当前分区的放置
        	//}else {
        		
        	//}
            if(assignment.getParameters()==null)
            assignment.setParameters(parameters);
            assignment.setNumber(topology.getNumber());
//            System.out.println("Run flow");
//            RunFlowsOnNetworkProcessor2 runFlowsOnNetworkProcessor2 = new RunFlowsOnNetworkProcessor2();
//            runFlowsOnNetworkProcessor2.process(topology, flows);

            WriterSerializableUtil.writeFile(assignment, new File(placementOutputFolder + "/assignment.txt"), false, parameters);
            successful = true;
            //now do postplacement
            Map<Partition, Map<Switch, Switch>> postPlaceResult = null;
            if(runNumber==1) {
            for (int i = 0; i < postPlacers.size(); i++) {
                String postPlacementFolder = outputFolder + "/" + i + "_postplacement";
                new File(postPlacementFolder).mkdirs();
                PrintWriter trendWriter = new PrintWriter(new BufferedWriter(new FileWriter(postPlacementFolder + "/" + "trend" + topologyName)));
                if (i == 0) {
                    postPlaceResult = doPostAssignment(topology, parameters, firstRun,
                            flows, postPlacementFolder, placer.getLastAvailableSwitches(), postPlacers.get(i),
                            trendWriter, assignment, topologyName);
                    
                } else {
                    Map<Partition, Switch> placement = new HashMap<Partition, Switch>(postPlaceResult.size());
                    for (Map.Entry<Partition, Map<Switch, Switch>> entry : postPlaceResult.entrySet()) {
                        placement.put(entry.getKey(), entry.getValue().values().iterator().next());
                    }
                    postPlaceResult = doPostAssignment(topology, parameters, firstRun, flows, postPlacementFolder,
                            topology.getSwitches(), postPlacers.get(i), trendWriter, new Assignment(placement), topologyName);
                }
                //NowpostPlaceResult = postPlaceResult;
                Writetofile(postPlaceResult);
                NowpostPlaceResult = Readfile("c://b.txt");
                trendWriter.close();
             // if(runNumber==1) {
               parameters.put("edgeMemory", "1");
               writePostPlaceResult(postPlaceResult, new File(postPlacementFolder + "/" + "assignment_1.txt"), parameters);
             //}else {
                //changeparameters
            	//parameters.put("edgeMemory", String.valueOf(runNumber));
                //writePostPlaceResult(postPlaceResult, new File(postPlacementFolder + "/" + "assignment_" + runNumber+".txt"), parameters);

             }

            }else {
            	
            }
            
            for (PostPlacer postPlacer : postPlacers) {
                if (postPlacer instanceof InformOnRestart) {
                    ((InformOnRestart) postPlacer).restart();

                }
            }

        } catch (NoAssignmentFoundException e) {
            e.printStackTrace();
            Util.logger.warning("No assignment found for " + Statistics.getParameterLine(parameters));
        }

        MultiplePlacementScript.writeSolutionCSV(parameters, firstRun, outputFolder, successful);
        topology.reset();

        return successful;
    }



	private static void Writetofile(Map<Partition, Map<Switch, Switch>> postPlaceResult) {
		// TODO Auto-generated method stub
    	  Map<Switch, Collection<Partition>> rPlacement = new HashMap<>();
    	  try {
    	  BufferedWriter bw = new BufferedWriter(new FileWriter("c:\\b.txt"));
          for (Map.Entry<Partition, Map<Switch, Switch>> entry1 : postPlaceResult.entrySet()) {
        	
        		    Switch Migrateswitch = null;
        		    Switch Originalwitch = null;
        		    Map<Switch, Switch> entry_1 =entry1.getValue();
        			for (Entry<Switch, Switch> entry_2 : entry_1.entrySet()) { 
                		Migrateswitch = entry_2.getValue();
                		Originalwitch = entry_2.getKey();
                	}	
        		  
        			bw.write(entry1.getKey().getId()+","+Originalwitch.getId()+","+Migrateswitch.getId());            		
           
        		    
        		    bw.newLine();
        		    bw.flush();
        		    
        		
          }
          bw.close();
          } catch (IOException e) {
  		    // TODO Auto-generated catch block
  		    e.printStackTrace();
  		} 
	}
	

    private static Map<Partition, Map<Switch, Switch>> Readfile(String fileName) {
		// TODO Auto-generated method stub
    	
    	Map<Partition, Map<Switch, Switch>> NowpostPlaceResult = new HashMap<Partition, Map<Switch, Switch>>();
    	File file = new File(fileName);  
        BufferedReader reader = null;  
        try {  
            //System.out.println("以行为单位读取文件内容，一次读一整行：");  
            reader = new BufferedReader(new FileReader(file));  
            String tempString = null;  
            int line = 1;  
            // 一次读入一行，直到读入null为文件结束  
            while ((tempString = reader.readLine()) != null) {  
                // 显示行号 
            	Map<Switch, Switch> switchmap = new HashMap<Switch, Switch>();
                //System.out.println("line " + line + ": "+ tempString);  
                String[] datas = tempString.split(",");
                Integer idnumber = Integer.valueOf(datas[0]);
                switchmap.put(Switchid.get(datas[1]), Switchid.get(datas[2]));
                NowpostPlaceResult.put(Partitionid.get(idnumber),switchmap);
                line++;  
            }  
            reader.close();  
        } catch (IOException e) {  
            e.printStackTrace();  
        } finally {  
            if (reader != null) {  
                try {  
                    reader.close();  
                } catch (IOException e1) {  
                }  
            }  
        }
		return NowpostPlaceResult;  
	}

	public static void writePostPlaceResult(Map<Partition, Map<Switch, Switch>> postPlaceResult, File outputFile, Map<String, Object> parameters) throws IOException {
        Map<Switch, Collection<Partition>> rPlacement = new HashMap<>();
        for (Map.Entry<Partition, Map<Switch, Switch>> entry1 : postPlaceResult.entrySet()) {
            for (Switch host : entry1.getValue().values()) {
                Collection<Partition> partitions = rPlacement.get(host);
                if (partitions == null) {
                    partitions = new LinkedList<>();
                    rPlacement.put(host, partitions);
                }
                partitions.add(entry1.getKey());
            }
        }

        PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(outputFile)));
        writer.println(Statistics.getParameterLine(parameters));
        for (Map.Entry<Switch, Collection<Partition>> entry : rPlacement.entrySet()) {
            writer.print(entry.getKey().getId());
            for (Partition partition : entry.getValue()) {
                writer.print("," + partition.getId());
            }
            writer.println();
        }
        writer.close();
    }

    private static Map<Partition, Map<Switch, Switch>> doPostAssignment(Topology topology, Map<String, Object> parameters, boolean firstRun,
                                                                        List<Flow> flows, String outputFolder, Collection<Switch> lastAvailableSwitches,
                                                                        PostPlacer postPlacer, PrintWriter postPlacementTrendWriter, Assignment assignment,
                                                                        String topologyName
    ) throws Exception {


        postPlacementTrendWriter.println(Statistics.getParameterLine(parameters));
        
        Map<Partition, Map<Switch, Switch>> partitionSourceReplica =
                postPlacer.postPlace(new HashSet<Switch>(lastAvailableSwitches), assignment, postPlacementTrendWriter);
        
        postPlacementTrendWriter.flush();

        postPlacementTrendWriter.println();
        postPlacementTrendWriter.flush();
        //new SetReplicateForwardingRulesProcessor().process(partitionSourceReplica, topology);
        System.out.println("Run flow");
        RunFlowsOnNetworkProcessor2 runFlowsOnNetworkProcessor2 = new RunFlowsOnNetworkProcessor2();
        runFlowsOnNetworkProcessor2.process(topology, flows);
        PrintWriter runFlowStatsWriter = new PrintWriter(new BufferedWriter(new FileWriter(outputFolder + "/postplacement_runflows_" + topologyName, false)), true);
        runFlowStatsWriter.println(Statistics.getParameterLine(parameters));


        runFlowsOnNetworkProcessor2.print(runFlowStatsWriter);
        runFlowStatsWriter.println();
        runFlowStatsWriter.close();

        Statistics postPlaceStats = topology.getStat(parameters);
        postPlaceStats.joinStats(postPlacer.getStats(parameters));

        new SaveFileProcessor<String>(Statistics.csvStatistics(parameters.keySet(), Statistics.categorize(parameters.keySet(),
                Collections.singleton(postPlaceStats)), postPlaceStats.getStatNames(), true, firstRun),
                new File(outputFolder + "/postplacement.csv"), !firstRun).run();
        return partitionSourceReplica;
    }

    public static Map<Partition, Long> getMinOverhead(Topology topology, Map<Partition, Map<Rule, Collection<Flow>>> classifiedFlows) {
        Map<Partition, Long> output = new HashMap<Partition, Long>();
        for (Map.Entry<Partition, Map<Rule, Collection<Flow>>> entry0 : classifiedFlows.entrySet()) {
            long sum = 0;
            for (Map.Entry<Rule, Collection<Flow>> entry : entry0.getValue().entrySet()) {
                for (Flow flow : entry.getValue()) {
                    if (entry.getKey().getAction().doAction(flow) != null) {
                        sum += topology.getPathLength(flow.getSource(), flow.getDestination()) * flow.getTraffic();
                    }
                }
            }
            output.put(entry0.getKey(), sum);
        }
        return output;
    }
    
    public static void copyFileUsingRedirection(File source, File dest) throws IOException {
        FileInputStream in = null;
        PrintStream out = null;
        try {
            in = new FileInputStream(source);
            out = new PrintStream(dest);
            System.setIn(in);
            System.setOut(out);
            Scanner sc = new Scanner(System.in);
            while(sc.hasNext()) {
                System.out.println(sc.nextLine());
            }
        } finally{
            if(in != null) {
                in.close();
            }
            if(out != null) {
                out.close();
         
            }
        }
        
        
    }


    public static void copyFile(File fromFile,File toFile) throws IOException{
        FileInputStream ins = new FileInputStream(fromFile);
        FileOutputStream out = new FileOutputStream(toFile);
        byte[] b = new byte[1024];
        int n=0;
        while((n=ins.read(b))!=-1){
            out.write(b, 0, n);
        }
        
        ins.close();
        out.close();
    }
    
    public static List<File> SortFile(List<File> topologyFiles){
    	List<File> ReturntopologyFiles = new LinkedList<File>();
    	File TempFile = null;
    	for(int i=0; i<topologyFiles.size()-1;i++){
    		
    		for(int j=0; j<topologyFiles.size();j++){
    			if(topologyFiles.get(j).getName().equals((i+1)+".txt")){
    				TempFile=topologyFiles.get(j);
    			}
    			//System.out.println(TempFile.getName());
    			
        	}
    		ReturntopologyFiles.add(TempFile);
    	}
        return ReturntopologyFiles;
    }
    
    
    public static <K, V extends Comparable<? super V>> Map<K, V> sortDescend(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new ArrayList<>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
            @Override
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                int compare = (o1.getValue()).compareTo(o2.getValue());
                return -compare;
            }
        });
 
        Map<K, V> returnMap = new LinkedHashMap<K, V>();
        for (Map.Entry<K, V> entry : list) {
            returnMap.put(entry.getKey(), entry.getValue());
        }
        return returnMap;
    }
 
    // Map的value值升序排序
    public static <K, V extends Comparable<? super V>> Map<K, V> sortAscend(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new ArrayList<Map.Entry<K, V>>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
            @Override
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                int compare = (o1.getValue()).compareTo(o2.getValue());
                return compare;
            }
        });
 
        Map<K, V> returnMap = new LinkedHashMap<K, V>();
        for (Map.Entry<K, V> entry : list) {
            returnMap.put(entry.getKey(), entry.getValue());
        }
        return returnMap;
    }

    public static Map<Switch, Set<Partition>> generateRPlacement(Map<Partition,Switch> placement) {
        Map<Switch, Set<Partition>> switchPartitionMap = new HashMap<Switch, Set<Partition>>();

        final HashSet<Switch> switches = new HashSet<Switch>(placement.values());
        for (Switch aSwitch : switches) {
            switchPartitionMap.put(aSwitch, new HashSet<Partition>());
        }
        for (Map.Entry<Partition, Switch> assignmentEntry : placement.entrySet()) {
            switchPartitionMap.get(assignmentEntry.getValue()).add(assignmentEntry.getKey());
        }
        return switchPartitionMap;
    }
    
    public static long getMinOverhead_1(Topology topology, Map<Partition, Map<Rule, Collection<Flow>>> classifiedFlows) {
        //Map<Partition, Long> output = new HashMap<Partition, Long>();
        long Allsum = 0;
        for (Map.Entry<Partition, Map<Rule, Collection<Flow>>> entry0 : classifiedFlows.entrySet()) {
            long sum = 0;
            for (Map.Entry<Rule, Collection<Flow>> entry : entry0.getValue().entrySet()) {
                for (Flow flow : entry.getValue()) {
                    if (entry.getKey().getAction().doAction(flow) != null) {
                        sum += topology.getPathLength(flow.getSource(), flow.getDestination()) * flow.getTraffic();
                    }
                }
            }
            Allsum=sum+Allsum;
        }
        return Allsum;
    }
    
}
