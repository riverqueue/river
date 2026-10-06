# frozen_string_literal: true

module River
  # Internal codec for the argument portion of Go-compatible unique keys.
  # Only top-level keys are sorted. Decoding and re-encoding values would lose
  # number lexemes, escaping, duplicate keys, and nested object ordering.
  module UniqueArgs
    def self.encode(json, fields)
      value = JSON.parse(json)
      if !fields.is_a?(Array) || fields.empty?
        return "{}" if value.is_a?(Array) && value.empty?
        raise ArgumentError, "unique args must encode a JSON object" unless value.is_a?(Hash)

        return encode_object(members(json).sort.to_h)
      end

      paths = fields.map do |field|
        path = Array(field).map(&:to_s)
        raise ArgumentError, "unique argument paths must not be empty" if path.empty?

        path
      end
      selected = {} #: Hash[String, untyped]
      paths.sort_by { |path| [path.join("."), path.map { |part| part.gsub(/([\\.])/) { |char| "\\" + char } }.join(".")] }.each do |path|
        raw = json
        path.each do |part|
          raw = members(raw)[part]
          break unless raw
        end
        next unless raw

        target = selected
        covered = false
        path.take(path.length - 1).each do |part|
          if target[part].is_a?(String)
            covered = true
            break # Selecting a whole object already includes its children.
          end
          target = (target[part] ||= {})
        end
        target[path.last] = raw unless covered
      end
      # Go's selected-field builder starts with nil bytes, not an empty object.
      selected.empty? ? "" : encode_object(selected)
    end

    # The caller validates JSON first. Values remain slices of the original
    # string; this is a token walk, not a second JSON decoder. Go takes the first
    # occurrence of a duplicate top-level key.
    def self.members(json)
      return {} unless json.lstrip.start_with?("{")

      result = {} #: Hash[String, String]
      depth = 0
      key = ""
      start = nil #: Integer?
      expecting_value = false
      # Anchoring prevents repeated searches through an unterminated string.
      # \K keeps leading whitespace out of the offsets used to slice raw values.
      json.scan(/\G\s*\K(?:"(?:[^"\\]|\\.)*"|[{}\[\],:]|[^\s{}\[\],:"]+)/) do |scanned|
        token = scanned #: String
        match = Regexp.last_match #: MatchData
        if depth == 1
          if start && [",", "}"].include?(token)
            raw = json[start...match.begin(0)] #: String
            result[key] ||= raw.rstrip
            start = nil
          elsif expecting_value
            start = match.begin(0)
            expecting_value = false
          elsif token == ":"
            expecting_value = true
          elsif token.start_with?('"') && !start
            key = JSON.parse(token)
          end
        end
        depth += 1 if token == "{" || token == "["
        depth -= 1 if token == "}" || token == "]"
      end
      result
    end

    def self.encode_object(object)
      "{" + object.map { |key, value| "#{encode_key(key)}:#{value.is_a?(Hash) ? encode_object(value) : value}" }.join(",") + "}"
    end

    # Match sjson's fast path for printable ASCII keys and encoding/json's
    # HTML escaping for all other keys. Values are never passed through here.
    def self.encode_key(key)
      return '"' + key + '"' unless key.match?(/[\x00-\x1f\u0080-\u{10ffff}"\\]/)

      JSON.generate(key).gsub(/[<>&\u2028\u2029]/) { |char| "\\u%04x" % char.ord }
    end

    private_class_method :encode_object, :encode_key
  end
end
