package com.multicloudpoc.pocmain.services;

import com.google.cloud.storage.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class GcsService {

    private final Storage storage;
    private final String bucketName;

    public GcsService(@Value("${bucket.name}")String bucketName) {
        this.bucketName = bucketName;
        // Initialize Google Cloud Storage client with application default credentials
        this.storage = StorageOptions.getDefaultInstance().getService();
        log.info("Using application default credentials: {}", StorageOptions.getDefaultInstance().getCredentials());

    }

    // Upload PDF file to GCP Storage bucket
    public void uploadPDF(MultipartFile file) throws IOException {
        Blob blob = storage.create(
                Blob.newBuilder(bucketName, file.getOriginalFilename())
                        .build(),
                file.getInputStream());
        log.info("File uploaded Successfully to GCS bucket: "+ bucketName);
    }

    // Download PDF file from GCP Storage bucket
    public byte[] downloadPDF(String filename) {
        Blob blob = storage.get(bucketName, filename);
        if (blob != null) {
            return blob.getContent();
        }
        log.info("File downloaded Successfully from GCS bucket: "+ bucketName);
        return null;
    }

    // List all PDF files in GCP Storage bucket
    public List<String> listPDFs() {
        Bucket bucket = storage.get(bucketName);
        List<String> pdfFiles = new ArrayList<>();
        if (bucket != null) {
            bucket.list().iterateAll().forEach(blob -> {
                if (blob.getName().endsWith(".pdf")) {
                    pdfFiles.add(blob.getName());
                }
            });
        }
        log.info("List of files in the GCS bucket: " + bucketName +" displayed");
        return pdfFiles;
    }

    // Generate signed URL for accessing a file in GCP Storage bucket
    public String generateSignedUrl(String filename) {
        try {
            BlobInfo blobInfo = BlobInfo.newBuilder(bucketName, filename).build();
            return storage.signUrl(blobInfo, 1L, TimeUnit.HOURS).toString(); // Adjust expiration time as needed
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    // Delete PDF file from GCP Storage bucket
    public boolean deletePDF(String filename) {
        BlobId blobId = BlobId.of(bucketName, filename);
        return storage.delete(blobId);
    }
}