# method description
def check_04(handles:, maximum_score:, resources:)
  bigquery = handles['project_0.BigqueryV2']
  ret_hash = { :score => 0, :message => "", :student_message => "" }
  isAvailable = false

  begin
    model = bigquery.get_model("cymbal".to_s, "gemini_flash_model".to_s) || []
  rescue 
    isAvailable = false
  end 

  if model
    isAvailable = true
  end

  if isAvailable 
    success_message = "모델이 정상적으로 생성되었습니다."
    ret_hash = { :done => true, :score => maximum_score, :message => success_message, :student_message => success_message}
  else
    error_message = '다시 한 번 절차를 확인해보세요!'
    ret_hash[:message] = error_message
    ret_hash[:student_message] = error_message
  end 
  return ret_hash
end