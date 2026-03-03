variable "name_prefix"          { type = string }
variable "vpc_id"               { type = string; default = "" }
variable "vpc_cidr"             { type = string; default = "10.0.0.0/16" }
variable "private_subnet_ids"   { type = list(string); default = [] }
variable "availability_zones"   { type = list(string) }
