class GraknCore < Formula
  desc "Grakn Core: The Knowledge Graph"
  homepage "https://grakn.ai"
  url "https://github.com/graknlabs/grakn/releases/download/{version}/grakn-core-all-mac-{version}.zip"
  sha256 "{sha256}"

  bottle :unneeded

  depends_on :java => "1.8"

  def setup_directory(dir)
    grakn_dir = var/name/dir
    grakn_dir.mkpath
    orig_dir = libexec/"server"/dir
    rm_rf orig_dir
    ln_s grakn_dir, orig_dir
  end

  def install
    libexec.install Dir["*"]
    setup_directory "db"
    setup_directory "logs" 
    bin.install libexec/"grakn"
    bin.env_script_all_files(libexec, Language::Java.java_home_env("1.8"))
  end

  test do
    assert_match /RUNNING/i, shell_output("#{bin}/grakn server status")
  end
end

