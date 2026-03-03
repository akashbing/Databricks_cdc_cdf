variable "name_prefix"          { type = string }
variable "suffix"               { type = string }
variable "kms_key_arn"          { type = string; default = "" }
variable "glue_extra_py_files"  { type = list(string); default = [] }
