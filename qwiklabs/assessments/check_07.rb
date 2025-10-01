# method description
def check_07(handles:, maximum_score:, resources:)
  logging = handles['project_0.LoggingV2']

  ret_hash = { :score => 0, :message => "", :student_message => "" }
  isAvailable = false

  lab_access_time_in_sec = 54000
  lab_start_time = Time.at(Time.now.to_i - lab_access_time_in_sec)

  log_filters = [
    'timestamp>"' + lab_start_time.utc.iso8601 + '"',
    'protoPayload.methodName="google.cloud.dataform.v1beta1.Dataform.CreateRepository"',
    'protoPayload.serviceName="dataform.googleapis.com"'
  ]

  # Build log filter
  custom_filter = log_filters.join(" AND ")

  # List_log_entries_request_object
  list_log_entries_request_object = Google::Apis::LoggingV2::ListLogEntriesRequest.new

  # Parent resource to track
  list_log_entries_request_object.resource_names = ['projects/' + logging.project]

  list_log_entries_request_object.filter = custom_filter
  query_logs = logging.list_entry_log_entries(list_log_entries_request_object)&.entries || []

  if query_logs.count >= 2
    isAvailable = true
  end

  if isAvailable 
    success_message = "정상적으로 노트북이 업로드 되었습니다."
    ret_hash = { :done => true, :score => maximum_score, :message => success_message, :student_message => success_message }
  else
    error_message = "다시 한 번 절차를 확인해보세요!"
    ret_hash[:message] = error_message
    ret_hash[:student_message] = error_message
  end
  return ret_hash
end