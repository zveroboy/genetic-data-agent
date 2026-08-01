use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UserVariant {
    pub chrom: String,
    pub pos: u32,
    pub rsid: String,
    pub ref_allele: String,
    pub alt_allele: String,
    pub gt_raw: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressPayload {
    pub stage: String,
    pub processed: usize,
    pub total: usize,
    pub percentage: String,
}

impl UserVariant {
    pub fn from_vcf_line(line: &str) -> Option<Self> {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            return None;
        }

        let parts: Vec<&str> = trimmed.split('\t').collect();
        if parts.len() < 10 {
            return None;
        }

        let chrom = parts[0].to_string();
        let pos = parts[1].parse::<u32>().ok()?;
        let rsid = parts[2].to_string();
        let ref_allele = parts[3].to_string();
        let alt_allele = parts[4].to_string();
        let format_field = parts[8];
        let sample_field = parts[9];

        let gt_idx = format_field
            .split(':')
            .position(|k| k == "GT")?;
        let gt_raw = sample_field
            .split(':')
            .nth(gt_idx)?
            .to_string();

        Some(Self {
            chrom,
            pos,
            rsid,
            ref_allele,
            alt_allele,
            gt_raw,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vcf_line_parsing() {
        let line = "15\t74749576\trs762551\tA\tC\t99\tPASS\tGENE=CYP1A2\tGT\t1/1";
        let variant = UserVariant::from_vcf_line(line).expect("Should parse valid VCF line");
        assert_eq!(variant.rsid, "rs762551");
        assert_eq!(variant.ref_allele, "A");
        assert_eq!(variant.alt_allele, "C");
        assert_eq!(variant.gt_raw, "1/1");
    }

    #[test]
    fn test_vcf_header_ignored() {
        assert!(UserVariant::from_vcf_line("#CHROM\tPOS\tID\tREF\tALT").is_none());
    }
}

