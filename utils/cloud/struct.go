package utils

type UploadParam struct {
	URL       string
	Bucket    string
	AccessKey string
	ACLPublic bool
	SecretKey string
	Region    string
	PathStyle bool
}
