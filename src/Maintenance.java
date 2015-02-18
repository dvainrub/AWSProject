import java.io.File;


public class Maintenance {

	public static void main(String[] args) {
		AWSObject awsO = new AWSObject();
		awsO.AWSInitAllServices();


			upload(awsO);

		delete(awsO);

		System.out.println("\n -------DONE--------");
	}

	private static void upload(AWSObject awsO) {
		uploadScripts(awsO);

		uploadJars(awsO);
	}

	private static void uploadScripts(AWSObject awsO) {
		File managerScriptFile = new File("managerscript.txt");
		awsO.S3UploadFile(StaticVars.APPLICATION_CODE_BUCKET_NAME, StaticVars.MANAGER_SCRIPT, managerScriptFile);
		System.out.println("Manager Script Uploaded");

		File workerScriptFile = new File("workerscript.txt");
		awsO.S3UploadFile(StaticVars.APPLICATION_CODE_BUCKET_NAME, StaticVars.WORKER_SCRIPT, workerScriptFile);
		System.out.println("Worker Script Uploaded");

	}

	private static void uploadJars(AWSObject awsO) {
		File localFile = new File("localapp.jar");
		awsO.S3UploadFile(StaticVars.APPLICATION_CODE_BUCKET_NAME, "localapp.jar", localFile);
		System.out.println("LocalApplication Jar Uploaded");

		File managerFile = new File("managerapp.jar");
		awsO.S3UploadFile(StaticVars.APPLICATION_CODE_BUCKET_NAME, "managerapp.jar", managerFile);
		System.out.println("Manager Jar Uploaded");

		File workerFile = new File("workerapp.jar");
		awsO.S3UploadFile(StaticVars.APPLICATION_CODE_BUCKET_NAME, "workerapp.jar", workerFile);
		System.out.println("Worker Jar Uploaded");

		System.out.println("!!!!DONT FORGET TO CHANGE THE PERMISSIONS->POLICY OF THE BUCKET!!!");
		/*
		 {

"Id": "Policy1424209498483",

"Statement": [

{

"Sid": "Stmt1424209492671",

"Action": [

"s3:GetObject"

],

"Effect": "Allow",

"Resource": "arn:aws:s3:::applicationcode-ds-151-elidor/*",

"Principal": "*"

}

]

}


		 */

	}

	private static void delete(AWSObject awsO) {
		deleteS3Buckets(awsO);

		try {
			awsO.SQSDeleteAllQueues();
		} catch (Exception e) {}
		System.out.println("All Queues deleted");

		awsO.EC2TerminateAllInstances();
		System.out.println("All Instances in EC2 deleted");		
	}

	private static void deleteS3Buckets(AWSObject awsO) {
		try {
			awsO.S3ForceDeleteBucket(StaticVars.INPUT_BUCKET_NAME);
			System.out.println("Input bucket deleted");
		} catch (Exception e) {}

		try{
			awsO.S3ForceDeleteBucket(StaticVars.OUTPUT_BUCKET_NAME);
			System.out.println("Output bucket deleted");
		} catch (Exception e){}

		try{
			/*awsO.S3ForceDeleteBucket(staticVars.APPLICATION_CODE_BUCKET_NAME);
			System.out.println("ApplicationCode bucket deleted");*/
		} catch (Exception e){}
	}

}
