# frozen_string_literal: true

D = Steep::Diagnostic

target :lib do
  check "lib"

  library "digest"
  library "json"
  library "logger"
  library "optparse"
  library "securerandom"
  library "socket"
  library "time"
  library "timeout"

  signature "sig"

  configure_code_diagnostics(D::Ruby.strict)
end
