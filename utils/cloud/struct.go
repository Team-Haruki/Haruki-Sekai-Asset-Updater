package utils

type UploadParam struct {
	Endpoint    string
	SSL         bool
	Bucket      string
	AccessKey   string
	ACLPublic   bool
	SecretKey   string
	Region      string
	PathStyle   bool
	RemoveLocal bool
}
