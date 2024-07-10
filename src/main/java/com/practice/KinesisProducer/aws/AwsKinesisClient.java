package com.practice.KinesisProducer.aws;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;

public class AwsKinesisClient {
	
	public static final String AWS_ACCESS_KEY="aws.accessKeyId";
	public static final String AWS_SECRET_KEY="aws.secretKey";
	
	static {
		System.setProperty(AWS_ACCESS_KEY, "AKIAQE464U4YIP72UJNQ");
		System.setProperty(AWS_SECRET_KEY, "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");
	}

	public static AmazonKinesis getKinesisClient() {
		return AmazonKinesisClientBuilder.standard()
				.withRegion(Regions.AP_SOUTH_1)
				.build();
	}
}
