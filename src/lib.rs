use hdt::containers::ControlType;
use hdt::containers::vbyte::encode_vbyte;
use hdt::header::Header;
use hdt::triples::Order;
use log::{debug, error, warn};
use oxrdf::vocab::rdf;
use oxrdf::{BlankNodeRef, Literal, NamedNode, NamedNodeRef, Term, Triple};
use oxrdfio::RdfFormat::{self, NTriples};
use oxrdfio::RdfSerializer;
use oxrdfio::{RdfParseError, RdfParser};
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::path::Path;

const HDT_CONTAINER: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/HDT/hdt#HDTv1");
const VOID_TRIPLES: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://rdfs.org/ns/void#triples");
const VOID_PROPERTIES: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://rdfs.org/ns/void#properties");
const VOID_DISTINCT_SUBJECTS: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://rdfs.org/ns/void#distinctSubjects");
const VOID_DISTINCT_OBJECTS: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://rdfs.org/ns/void#distinctObjects");
const VOID_DATASET: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://rdfs.org/ns/void#Dataset");
const HDT_STATISTICAL_INFORMATION: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/HDT/hdt#statisticalInformation");
const HDT_PUBLICATION_INFORMATION: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/HDT/hdt#publicationInformation");
const HDT_FORMAT_INFORMATION: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/HDT/hdt#formatInformation");
const HDT_DICTIONARY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/HDT/hdt#dictionary");
const HDT_TRIPLES: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/HDT/hdt#triples");
const DC_TERMS_FORMAT: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/dc/terms/format");
const HDT_NUM_TRIPLES: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/HDT/hdt#triplesnumTriples");
const HDT_TRIPLES_ORDER: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/HDT/hdt#triplesOrder");
const HDT_ORIGINAL_SIZE: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/HDT/hdt#originalSize");
const HDT_SIZE: NamedNodeRef<'_> = NamedNodeRef::new_unchecked("http://purl.org/HDT/hdt#hdtSize");
const DC_TERMS_ISSUED: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/dc/terms/issued");
const HDT_DICT_SHARED_SO: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/HDT/hdt#dictionarynumSharedSubjectObject");
const HDT_DICT_MAPPING: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/HDT/hdt#dictionarymapping");
const HDT_DICT_SIZE_STRINGS: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/HDT/hdt#dictionarysizeStrings");
const HDT_DICT_BLOCK_SIZE: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/HDT/hdt#dictionaryblockSize");
const HDT_TYPE_BITMAP: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/HDT/hdt#triplesBitmap");
const HDT_DICTIONARY_TYPE_FOUR: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/HDT/hdt#dictionaryFour");

#[derive(Debug)]
pub struct EncodedTripleId {
    subject: u32,
    predicate: u32,
    object: u32,
}

#[derive(Default, Debug)]
pub struct ConvertedHDT {
    pub dict: FourSectionDictionary,
    pub triples: BitmapTriples,
    header: Header,
    num_triples: usize,
}

/// Represents a compressed LogSequence2 sequence for storage
pub struct LogSequence2 {
    compressed_terms: Vec<u8>,
    offsets: Vec<u32>, // Stores positions of terms
    num_terms: usize,
}

impl LogSequence2 {
    /// Compress a sorted vector of terms using prefix compression
    pub fn compress(set: &BTreeSet<String>) -> Result<Self, Box<dyn Error>> {
        let mut terms: Vec<String> = set.iter().to_owned().cloned().collect();
        terms.sort(); // Ensure lexicographic order
        // println!("{:?}", terms);
        let mut compressed_terms = Vec::new();
        let mut offsets = Vec::new();
        let mut last_term = "";

        let num_terms = terms.len();
        let block_size = 16; // Every 16th term is stored fully
        for (i, term) in terms.iter().enumerate() {
            if i % block_size == 0 {
                offsets.push(compressed_terms.len() as u32);
                compressed_terms.extend_from_slice(term.as_bytes());
                // Every block stores a full term
            } else {
                let common_prefix_len = last_term
                    .chars()
                    .zip(term.chars())
                    .take_while(|(a, b)| a == b)
                    .count();
                compressed_terms.extend_from_slice(&encode_vbyte(common_prefix_len));
                compressed_terms.extend_from_slice(term[common_prefix_len..].as_bytes());
            };

            compressed_terms.push(0); // Null separator

            last_term = term;
        }
        offsets.push(compressed_terms.len() as u32);

        Ok(Self {
            compressed_terms,
            offsets,
            num_terms,
        })
    }

    /// Save the LogSequence2Rust structure to a file
    pub fn save(&self, dest_writer: &mut BufWriter<File>) -> Result<(), Box<dyn Error>> {
        let crc = crc::Crc::<u8>::new(&crc::CRC_8_SMBUS);
        let mut hasher = crc.digest();
        // libhdt/src/libdcs/CSD_PFC.cpp::save()
        // save type
        let seq_type: [u8; 1] = [2];
        let _ = dest_writer.write(&seq_type)?;
        hasher.update(&seq_type);

        // // Save sizes
        let mut buf: Vec<u8> = vec![];
        buf.extend_from_slice(&encode_vbyte(self.num_terms));
        buf.extend_from_slice(&encode_vbyte(self.compressed_terms.len()));
        buf.extend_from_slice(&encode_vbyte(16));
        let _ = dest_writer.write(&buf)?;
        hasher.update(&buf);
        let checksum = hasher.finalize();
        let _ = dest_writer.write(&checksum.to_le_bytes())?;

        // // Write number of terms
        save_u32_vec(&self.offsets, dest_writer)?;

        // Write packed data
        let crc = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);
        let mut hasher = crc.digest();
        let _ = dest_writer.write(&self.compressed_terms)?;
        hasher.update(&self.compressed_terms);
        // println!("{}", String::from_utf8_lossy(&self.compressed_terms));
        let checksum = hasher.finalize();
        let _ = dest_writer.write(&checksum.to_le_bytes())?;

        Ok(())
    }
}

fn save_u32_vec(ints: &[u32], dest_writer: &mut BufWriter<File>) -> Result<(), Box<dyn Error>> {
    let crc = crc::Crc::<u8>::new(&crc::CRC_8_SMBUS);
    let mut hasher = crc.digest();
    // libhdt/src/sequence/LogSequence2.cpp::save()
    // Write offsets using variable-length encoding
    let seq_type: [u8; 1] = [1];
    let _ = dest_writer.write(&seq_type)?;
    hasher.update(&seq_type);
    // Write numbits
    let bits_per_entry: [u8; 1] = [32];
    let _ = dest_writer.write(&bits_per_entry)?;
    hasher.update(&bits_per_entry);
    // Write numentries
    let buf = &encode_vbyte(ints.len());
    let _ = dest_writer.write(buf)?;
    hasher.update(buf);
    let checksum = hasher.finalize();
    let _ = dest_writer.write(&checksum.to_le_bytes())?;

    // Write data
    let crc = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);
    let mut hasher = crc.digest();
    let offset_data = convert_vec_u32_to_vec_u8(ints);
    let _ = dest_writer.write(&offset_data)?;
    hasher.update(&offset_data);
    let checksum = hasher.finalize();
    let _ = dest_writer.write(&checksum.to_le_bytes())?;

    Ok(())
}

fn convert_vec_u32_to_vec_u8(ints: &[u32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(ints.len() * 4);
    for offset in ints {
        bytes.extend_from_slice(&offset.to_le_bytes());
    }
    bytes
}

#[derive(Default, Debug)]
pub struct FourSectionDictionary {
    so_id_map: HashMap<String, u32>,
    pred_id_map: HashMap<String, u32>,
    subject_id_map: HashMap<String, u32>,
    object_id_map: HashMap<String, u32>,

    shared_terms: BTreeSet<String>,
    subject_terms: BTreeSet<String>,
    object_terms: BTreeSet<String>,
    predicate_terms: BTreeSet<String>,

    size_strings: usize,
}

#[derive(PartialEq)]
enum DictionaryRole {
    Subject,
    Predicate,
    Object,
}

/// Convert triple string formats from OxRDF to HDT.
fn term_to_hdt_bgp_str(term: &Term) -> Result<String, Box<dyn Error>> {
    let hdt_str = match term {
        // hdt terms should not include < >'s from IRIs
        Term::NamedNode(named_node) => named_node.clone().into_string(),

        Term::Literal(literal) => literal.to_string(),

        Term::BlankNode(_s) => term.to_string(),
    };

    Ok(hdt_str)
}

impl FourSectionDictionary {
    // fn number_of_elements(&self) -> usize {
    //     self.so_terms.len()
    //         + self.subject_terms.len()
    //         + self.predicate_terms.len()
    //         + self.object_terms.len()
    // }

    fn load(nt_file: &str) -> Result<(Self, Vec<EncodedTripleId>), Box<dyn Error>> {
        let source = match std::fs::File::open(nt_file) {
            Ok(f) => f,
            Err(e) => {
                error!("Error opening file {:?}: {:?}", nt_file, e);
                return Err(e.into());
            }
        };
        let source_reader = BufReader::new(source);
        let mut triples = vec![];
        let quads = RdfParser::from_format(NTriples).for_reader(source_reader);

        let mut subject_terms = BTreeSet::new();
        let mut object_terms = BTreeSet::new();
        let mut dict = FourSectionDictionary::default();
        for q in quads {
            let q = q?; //propagate the error  

            subject_terms.insert(term_to_hdt_bgp_str(&q.subject.into())?);
            dict.predicate_terms
                .insert(term_to_hdt_bgp_str(&q.predicate.into())?);
            object_terms.insert(term_to_hdt_bgp_str(&q.object)?);
        }

        dict.shared_terms = subject_terms.intersection(&object_terms).cloned().collect();
        dict.subject_terms = subject_terms
            .difference(&dict.shared_terms)
            .cloned()
            .collect();
        dict.object_terms = object_terms
            .difference(&dict.shared_terms)
            .cloned()
            .collect();

        // Shared subject-objects: 1..=|SOG|
        let mut shared_id = 1;
        for term in &dict.shared_terms {
            dict.so_id_map.insert(term.clone(), shared_id);
            shared_id += 1;
        }

        // Subject-only: |SOG|+1 ..= |SG|
        let mut id = shared_id;
        for term in &dict.subject_terms {
            dict.subject_id_map.insert(term.clone(), id);
            id += 1;
        }

        // Object-only: |SOG|+1 ..= |OG|
        let mut id = shared_id;
        for term in &dict.object_terms {
            dict.object_id_map.insert(term.clone(), id);
            id += 1;
        }

        // Predicates: 1..=|PG|
        for (i, term) in dict.predicate_terms.iter().enumerate() {
            dict.pred_id_map.insert(term.clone(), (i + 1) as u32);
        }

        let source = match std::fs::File::open(nt_file) {
            Ok(f) => f,
            Err(e) => {
                error!("Error opening file {:?}: {:?}", nt_file, e);
                return Err(e.into());
            }
        };

        let source_reader = BufReader::new(source);
        let quads = RdfParser::from_format(NTriples).for_reader(source_reader);
        for q in quads {
            let q = q?; //propagate the error  
            triples.push(EncodedTripleId {
                subject: dict.term_to_id(
                    &term_to_hdt_bgp_str(&q.subject.into())?,
                    DictionaryRole::Subject,
                ),
                predicate: dict.term_to_id(
                    &term_to_hdt_bgp_str(&q.predicate.into())?,
                    DictionaryRole::Predicate,
                ),
                object: dict.term_to_id(
                    &term_to_hdt_bgp_str(&q.object)?,
                    DictionaryRole::Object,
                ),
            });
        }

        // println!("triples: {:?}", triples);
        Ok((dict, triples))
    }

    fn term_to_id(&self, term: &str, role: DictionaryRole) -> u32 {
        match role {
            DictionaryRole::Predicate => *self.pred_id_map.get(term).unwrap(),
            DictionaryRole::Subject => {
                if let Some(id) = self.so_id_map.get(term) {
                    *id
                } else {
                    *self.subject_id_map.get(term).unwrap()
                }
            }
            DictionaryRole::Object => {
                if let Some(id) = self.so_id_map.get(term) {
                    *id
                } else {
                    *self.object_id_map.get(term).unwrap()
                }
            }
        }
    }
}

impl ConvertedHDT {
    fn load(nt_file: &str) -> Result<Self, Box<dyn Error>> {
        let (dictionary, encoded_triples) = FourSectionDictionary::load(nt_file)?;
        let num_triples = encoded_triples.len();
        let bmap_triples = BitmapTriples::load(encoded_triples)?;

        let mut converted_hdt = ConvertedHDT {
            dict: dictionary,
            triples: bmap_triples,
            num_triples,
            ..Default::default()
        };
        converted_hdt.build_header(nt_file)?;

        Ok(converted_hdt)
    }

    pub fn save(&self, dest_file: &str) -> Result<(), Box<dyn Error>> {
        let file = File::create(dest_file)?;
        let mut dest_writer = BufWriter::new(file);

        // libhdt/src/hdt/BasicHDT.cpp::saveToHDT
        let ci = hdt::containers::ControlInfo {
            control_type: ControlType::Global,
            format: HDT_CONTAINER.to_string(),
            ..Default::default()
        };
        ci.save(&mut dest_writer)?;

        let mut ci = hdt::containers::ControlInfo {
            control_type: ControlType::Header,
            format: "ntriples".to_string(),
            ..Default::default()
        };
        let mut graph: oxrdf::Graph = oxrdf::Graph::new();
        for t in &self.header.fields {
            //string_size += dest_writer.write(format!("{} .", t).as_bytes())?;
            graph.insert(t);
        }
        let graph_string = graph.to_string();
        ci.properties
            .insert("length".to_string(), graph_string.len().to_string());
        ci.save(&mut dest_writer)?;
        let _ = dest_writer.write(graph_string.as_bytes())?;

        // libhdt/src/dictionary/FourSectionDictionary.cpp::save()
        let mut ci = hdt::containers::ControlInfo {
            control_type: ControlType::Dictionary,
            format: HDT_DICTIONARY_TYPE_FOUR.to_string(),
            ..Default::default()
        };
        ci.properties
            .insert("mappings".to_string(), "1".to_string());
        ci.properties.insert(
            "sizeStrings".to_string(),
            self.dict.size_strings.to_string(),
        );
        ci.save(&mut dest_writer)?;
        //shared
        let log_seq = LogSequence2::compress(&self.dict.shared_terms)?;
        log_seq.save(&mut dest_writer)?;
        //subjects
        let log_seq: LogSequence2 = LogSequence2::compress(&self.dict.subject_terms)?;
        log_seq.save(&mut dest_writer)?;
        //predicates
        let log_seq = LogSequence2::compress(&self.dict.predicate_terms)?;
        log_seq.save(&mut dest_writer)?;
        //objects
        let log_seq = LogSequence2::compress(&self.dict.object_terms)?;
        log_seq.save(&mut dest_writer)?;

        let mut ci = hdt::containers::ControlInfo {
            control_type: ControlType::Triples,
            format: HDT_TYPE_BITMAP.to_string(),
            ..Default::default()
        };
        ci.properties.insert(
            "order".to_string(),
            (self.triples.order.clone() as u8).to_string(),
        );
        ci.save(&mut dest_writer)?;
        self.triples.save(&mut dest_writer)?;
        dest_writer.flush()?;
        Ok(())
    }

    fn build_header(&mut self, source_file: &str) -> Result<(), Box<dyn Error>> {
        let mut h = Header::default();
        // libhdt/src/hdt/BasicHDT.cpp::fillHeader()

        // uint64_t origSize = header->getPropertyLong(statisticsNode.c_str(), HDTVocabulary::ORIGINAL_SIZE.c_str());

        // header->clear();
        let file_iri = format!(
            "file://{}",
            std::path::Path::new(source_file).canonicalize()?.display()
        );
        let base_iri = NamedNodeRef::new(&file_iri)?;
        // // BASE
        // header->insert(baseUri, HDTVocabulary::RDF_TYPE, HDTVocabulary::HDT_DATASET);
        h.fields
            .insert(Triple::new(base_iri, rdf::TYPE, HDT_CONTAINER));

        // // VOID
        // header->insert(baseUri, HDTVocabulary::RDF_TYPE, HDTVocabulary::VOID_DATASET);
        h.fields
            .insert(Triple::new(base_iri, rdf::TYPE, VOID_DATASET));
        // header->insert(baseUri, HDTVocabulary::VOID_TRIPLES, triples->getNumberOfElements());
        h.fields.insert(Triple::new(
            base_iri,
            VOID_TRIPLES,
            Literal::new_simple_literal(self.num_triples.to_string()),
        ));
        // header->insert(baseUri, HDTVocabulary::VOID_PROPERTIES, dictionary->getNpredicates());
        h.fields.insert(Triple::new(
            base_iri,
            VOID_PROPERTIES,
            Literal::new_simple_literal(self.dict.pred_id_map.len().to_string()),
        ));
        // header->insert(baseUri, HDTVocabulary::VOID_DISTINCT_SUBJECTS, dictionary->getNsubjects());
        h.fields.insert(Triple::new(
            base_iri,
            VOID_DISTINCT_SUBJECTS,
            Literal::new_simple_literal(
                (self.dict.subject_id_map.len() + self.dict.so_id_map.len()).to_string(),
            ),
        ));
        // header->insert(baseUri, HDTVocabulary::VOID_DISTINCT_OBJECTS, dictionary->getNobjects());
        h.fields.insert(Triple::new(
            base_iri,
            VOID_DISTINCT_OBJECTS,
            Literal::new_simple_literal(
                (self.dict.object_id_map.len() + self.dict.so_id_map.len()).to_string(),
            ),
        ));
        // // TODO: Add more VOID Properties. E.g. void:classes

        // // Structure
        let stats_id = BlankNodeRef::new("statistics")?;
        let pub_id = BlankNodeRef::new("publicationInformation")?;
        let format_id = BlankNodeRef::new("format")?;
        let dict_id = BlankNodeRef::new("dictionary")?;
        let triples_id = BlankNodeRef::new("triples")?;
        // header->insert(baseUri, HDTVocabulary::HDT_STATISTICAL_INFORMATION,	statisticsNode);
        h.fields
            .insert(Triple::new(base_iri, HDT_STATISTICAL_INFORMATION, stats_id));
        // header->insert(baseUri, HDTVocabulary::HDT_PUBLICATION_INFORMATION,	publicationInfoNode);
        h.fields
            .insert(Triple::new(base_iri, HDT_STATISTICAL_INFORMATION, pub_id));
        // header->insert(baseUri, HDTVocabulary::HDT_FORMAT_INFORMATION, formatNode);
        h.fields
            .insert(Triple::new(base_iri, HDT_FORMAT_INFORMATION, format_id));
        // header->insert(formatNode, HDTVocabulary::HDT_DICTIONARY, dictNode);
        h.fields
            .insert(Triple::new(format_id, HDT_DICTIONARY, dict_id));
        // header->insert(formatNode, HDTVocabulary::HDT_TRIPLES, triplesNode);
        h.fields
            .insert(Triple::new(format_id, HDT_TRIPLES, triples_id));

        // DICTIONARY
        // header.insert(rootNode, HDTVocabulary::DICTIONARY_NUMSHARED, getNshared());
        h.fields.insert(Triple::new(
            dict_id,
            HDT_DICT_SHARED_SO,
            Literal::new_simple_literal(self.dict.so_id_map.len().to_string()),
        ));
        // header.insert(rootNode, HDTVocabulary::DICTIONARY_MAPPING, this->mapping);
        h.fields.insert(Triple::new(
            dict_id,
            HDT_DICT_MAPPING,
            Literal::new_simple_literal("2233"),
        ));
        // header.insert(rootNode, HDTVocabulary::DICTIONARY_SIZE_STRINGS, size());
        h.fields.insert(Triple::new(
            dict_id,
            HDT_DICT_SIZE_STRINGS,
            Literal::new_simple_literal("777"),
        ));
        // header.insert(rootNode, HDTVocabulary::DICTIONARY_BLOCK_SIZE, this->blocksize);
        h.fields.insert(Triple::new(
            dict_id,
            HDT_DICT_BLOCK_SIZE,
            Literal::new_simple_literal("16"), // TODO is this always 16?
        ));

        // TRIPLES
        // header.insert(rootNode, HDTVocabulary::TRIPLES_TYPE, getType());
        h.fields.insert(Triple::new(
            triples_id,
            DC_TERMS_FORMAT,
            NamedNode::new("http://purl.org/HDT/hdt#triplesBitmap")?,
        ));
        // header.insert(rootNode, HDTVocabulary::TRIPLES_NUM_TRIPLES, getNumberOfElements() );
        h.fields.insert(Triple::new(
            triples_id,
            HDT_NUM_TRIPLES,
            Literal::new_simple_literal(self.num_triples.to_string()),
        ));
        // header.insert(rootNode, HDTVocabulary::TRIPLES_ORDER, getOrderStr(order) );
        h.fields.insert(Triple::new(
            triples_id,
            HDT_TRIPLES_ORDER,
            Literal::new_simple_literal("SPO"),
        ));

        // // Sizes
        let meta = File::open(std::path::Path::new(source_file))?
            .metadata()
            .unwrap();
        // header->insert(statisticsNode, HDTVocabulary::ORIGINAL_SIZE, origSize);
        h.fields.insert(Triple::new(
            stats_id,
            HDT_ORIGINAL_SIZE,
            Literal::new_simple_literal(meta.len().to_string()),
        ));
        // header->insert(statisticsNode, HDTVocabulary::HDT_SIZE, getDictionary()->size() + getTriples()->size());
        h.fields.insert(Triple::new(
            stats_id,
            HDT_SIZE,
            Literal::new_simple_literal("222"),
        ));

        // // Current time
        // struct tm* today = localtime(&now);
        // strftime(date, 40, "%Y-%m-%dT%H:%M:%S%z", today);
        // header->insert(publicationInfoNode, HDTVocabulary::DUBLIN_CORE_ISSUED, date);
        let now = chrono::Utc::now(); // Get current local datetime
        let datetime_str = now.format("%Y-%m-%dT%H:%M:%S%z").to_string(); // Format as string
        h.fields.insert(Triple::new(
            pub_id,
            DC_TERMS_ISSUED,
            Literal::new_simple_literal(datetime_str),
        ));

        self.header = h;

        Ok(())
    }
}

pub fn build_hdt(file_paths: Vec<String>, dest_file: &str) -> Result<ConvertedHDT, Box<dyn Error>> {
    if file_paths.is_empty() {
        error!("no files provided");
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "no files provided to convert",
        )
        .into());
    }

    let nt_file = if file_paths.len() == 1 && file_paths[0].ends_with(".nt") {
        file_paths[0].clone()
    } else {
        let tmp_file = tempfile::Builder::new()
            .suffix(".nt")
            .keep(true)
            .tempfile()?;
        convert_to_nt(file_paths, tmp_file.reopen()?)?;
        tmp_file.path().to_str().unwrap().to_string()
    };

    let converted_hdt = ConvertedHDT::load(&nt_file)?;

    converted_hdt.save(dest_file)?;

    Ok(converted_hdt)
}

/// Function to sort a vector of Triples in SPO order
fn sort_triples_spo(triples: &mut [EncodedTripleId]) {
    triples.sort_by(spo_comparator);
}

fn spo_comparator(a: &EncodedTripleId, b: &EncodedTripleId) -> Ordering {
    let subject_order = a.subject.cmp(&b.subject);
    if subject_order != Ordering::Equal {
        return subject_order;
    }

    let predicate_order = a.predicate.cmp(&b.predicate);
    if predicate_order != Ordering::Equal {
        return predicate_order;
    }

    a.object.cmp(&b.object)
}

#[derive(Default, Debug)]
pub struct BitmapTriples {
    y_vec: Vec<u32>,
    z_vec: Vec<u32>,
    bitmap_y: Vec<bool>,
    bitmap_z: Vec<bool>,
    order: Order,
    num_triples: usize,
}

pub fn log2(n: usize) -> usize {
    if n != 0 {
        (usize::BITS - n.leading_zeros()) as usize
    } else {
        0
    }
}

impl BitmapTriples {
    /// Creates a new BitmapTriples from a list of sorted RDF triples
    pub fn load(mut triples: Vec<EncodedTripleId>) -> Result<Self, Box<dyn Error>> {
        // libhdt/src/triples/BitmapTriples.cpp:load()
        sort_triples_spo(&mut triples);
        let mut num_triples = 0;

        let mut y_bitmap = vec![];
        let mut z_bitmap = vec![];
        let mut array_y = Vec::new();
        let mut array_z = Vec::new();

        let mut last_x: u32 = 0;
        let mut last_y: u32 = 0;
        let mut last_z: u32 = 0;
        for triple in &triples {
            let x = triple.subject;
            let y = triple.predicate;
            let z = triple.object;

            if x == 0 || y == 0 || z == 0 {
                panic!("something is zero")
            }

            if num_triples == 0 {
                array_y.push(y);
                array_z.push(z);
            } else if x != last_x {
                if x != last_x + 1 {
                    panic!("bad x value")
                }

                //x unchanged
                y_bitmap.push(true);
                array_y.push(y);

                z_bitmap.push(true);
                array_z.push(z);
            } else if y != last_y {
                if y < last_y {
                    panic!("problem with y")
                }

                // y unchanged
                y_bitmap.push(false);
                array_y.push(y);

                z_bitmap.push(true);
                array_z.push(z);
            } else {
                if z < last_z {
                    panic!("bad z")
                }

                // z changed
                z_bitmap.push(false);
                array_z.push(z);
            }

            last_x = x;
            last_y = y;
            last_z = z;

            num_triples += 1;
        }

        y_bitmap.push(true);
        z_bitmap.push(true);

        Ok(BitmapTriples {
            bitmap_y: y_bitmap,
            bitmap_z: z_bitmap,
            y_vec: array_y,
            z_vec: array_z,
            order: Order::SPO,
            num_triples: triples.len(),
        })
    }

    fn save(&self, dest_writer: &mut BufWriter<File>) -> Result<(), Box<dyn Error>> {
        self.save_bitmap(&self.bitmap_y, dest_writer)?;

        // bitmapZ->save(output);
        self.save_bitmap(&self.bitmap_z, dest_writer)?;

        // arrayY->save(output);
        save_u32_vec(&self.y_vec, dest_writer)?;
        // // libhdt/src/sequence/LogSequence2.cpp::save()
        save_u32_vec(&self.z_vec, dest_writer)?;

        Ok(())
    }

    fn save_bitmap(
        &self,
        v: &[bool],
        dest_writer: &mut BufWriter<File>,
    ) -> Result<(), Box<dyn Error>> {
        // libhdt/src/bitsequence/BitSequence375.cpp::save()
        let crc = crc::Crc::<u8>::new(&crc::CRC_8_SMBUS);
        let mut hasher = crc.digest();
        // type
        let bitmap_type: [u8; 1] = [1];
        let _ = dest_writer.write(&bitmap_type)?;
        hasher.update(&bitmap_type);
        // number of bits
        let t = encode_vbyte(v.len());
        let _ = dest_writer.write(&t)?;
        hasher.update(&t);
        // crc8 checksum
        let checksum = hasher.finalize();
        let _ = dest_writer.write(&checksum.to_le_bytes())?;

        // write data
        let crc = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);
        let mut hasher = crc.digest();
        let buf = byte_align_bitmap(v);
        let _ = dest_writer.write(&buf)?;
        hasher.update(&buf);
        let checksum = hasher.finalize();
        let _ = dest_writer.write(&checksum.to_le_bytes())?;
        Ok(())
    }
}

fn byte_align_bitmap(bits: &[bool]) -> Vec<u8> {
    let mut byte = 0u8;
    let mut bit_index = 0;
    let mut byte_vec = Vec::new();

    for &bit in bits {
        if bit {
            byte |= 1 << bit_index;
        }
        bit_index += 1;

        if bit_index == 8 {
            byte_vec.push(byte);
            byte = 0;
            bit_index = 0;
        }
    }

    // If remaining bits exist, pad the last byte
    if bit_index > 0 {
        byte_vec.push(byte);
    }
    byte_vec
}

fn usize_to_u8_array(val: usize) -> [u8; 1] {
    if val > 255 {
        warn!("{val} greater than 255");
    }
    [(val & 0xFF) as u8] // Extracts the least significant byte
}

fn bits(n: usize) -> usize {
    if n == 0 {
        0
    } else {
        (usize::BITS - n.leading_zeros()) as usize
    }
}

fn bytes_for_bitmap(bits: usize) -> usize {
    if bits == 0 {
        return 1;
    }
    ((bits - 1) >> 3) + 1
}

fn convert_to_nt(
    file_paths: Vec<String>,
    output_file: std::fs::File,
) -> Result<(), Box<dyn Error>> {
    let mut dest_writer = BufWriter::new(output_file);
    for file in file_paths {
        let source = match std::fs::File::open(&file) {
            Ok(f) => f,
            Err(e) => {
                error!("Error opening file {:?}: {:?}", file, e);
                return Err(e.into());
            }
        };
        let source_reader = BufReader::new(source);

        debug!("converting {} to nt format", &file);

        let mut serializer = RdfSerializer::from_format(NTriples).for_writer(dest_writer.by_ref());
        let v = std::time::Instant::now();
        let rdf_format = if let Some(t) =
            RdfFormat::from_extension(Path::new(&file).extension().unwrap().to_str().unwrap())
        {
            t
        } else {
            error!("unrecognized file extension for {file}");
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unrecognized file extension for {file}"),
            )
            .into());
        };
        let quads = RdfParser::from_format(rdf_format).for_reader(source_reader);
        for q in quads {
            let q = match q {
                Ok(v) => v,
                Err(e) => {
                    match e {
                        RdfParseError::Io(v) => {
                            // I/O error while reading file
                            error!("Error reading file {file}: {v}");
                            return Err(v.into());
                        }
                        RdfParseError::Syntax(syn_err) => {
                            error!("syntax error for RDF file {file}: {syn_err}");
                            return Err(syn_err.into());
                        }
                    }
                }
            };
            if q.graph_name != oxrdf::GraphName::DefaultGraph {
                warn!("HDT does not support named graphs, merging triples for {file}");
            }
            serializer.serialize_triple(oxrdf::TripleRef {
                subject: q.subject.as_ref(),
                predicate: q.predicate.as_ref(),
                object: q.object.as_ref(),
            })?
        }

        serializer.finish()?;
        debug!("Convert time: {:?}", v.elapsed());
    }
    dest_writer.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {

    use std::{fs::remove_file, io::Read};

    use super::*;
    use hdt::containers::ControlInfo;
    use std::sync::Arc;

    #[test]
    fn test_rdf() {
        let mut tmp_file = tempfile::Builder::new().suffix(".nt").tempfile().expect("");
        assert!(
            (convert_to_nt(
                vec!["tests/resources/apple.ttl".to_string()],
                tmp_file.reopen().expect("")
            ))
            .is_ok()
        )
    }

    #[test]
    fn test_build_hdt() {
        let output_file = "test.hdt";
        let _ = remove_file(output_file);

        let res = build_hdt(vec!["tests/resources/apple.ttl".to_string()], output_file);
        assert!(res.is_ok());
        let conv_hdt = res.unwrap();

        let p = Path::new(output_file);
        assert!(p.exists());
        let source = std::fs::File::open(p).expect("failed to open hdt file");
        let mut hdt_reader = BufReader::new(source);

        let ci = ControlInfo::read(&mut hdt_reader).expect("failed to read HDT control info");
        let h = Header::read(&mut hdt_reader).expect("failed to read HDT Header");

        let unvalidated_dict = hdt::four_sect_dict::FourSectDict::read(&mut hdt_reader)
            .expect("failed to read dictionary");
        let dict = unvalidated_dict
            .validate()
            .expect("invalid 4 section dictionary");
        assert_eq!(
            dict.objects.num_strings(),
            conv_hdt.dict.object_id_map.len()
        );
        assert_eq!(
            dict.subjects.num_strings(),
            conv_hdt.dict.subject_id_map.len()
        );
        assert_eq!(
            dict.predicates.num_strings(),
            conv_hdt.dict.pred_id_map.len()
        );
        assert_eq!(dict.shared.num_strings(), conv_hdt.dict.so_id_map.len());

        let triples = hdt::triples::TriplesBitmap::read_sect(&mut hdt_reader)
            .expect("invalid bitmap triples");
        let mut buffer = [0; 1024];
        assert!(hdt_reader.read(&mut buffer).expect("failed to read") == 0);

        let source = std::fs::File::open(p).expect("failed to open hdt file");
        let hdt_reader = BufReader::new(source);
        let h = hdt::Hdt::new(hdt_reader).expect("failed to load HDT file");
        let t: Vec<(Arc<str>, Arc<str>, Arc<str>)> = h.triples().collect();
        println!("{:?}", t);
        assert_eq!(t.len(), 9);

        // http://example.org/apple#Apple,http://example.org/apple#color,Red
        let s = "http://example.org/apple#Apple";
        let p = "http://example.org/apple#color";
        let o = "\"Red\"";
        let triple_vec = vec![(Arc::from(s), Arc::from(p), Arc::from(o))];

        let res = h
            .triples_with_pattern(None, Some(p), Some(o))
            .collect::<Vec<_>>();
        assert_eq!(triple_vec, res)
    }

    #[test]
    fn test_read_hdt() {
        let input_file = "tests/resources/apple.hdt";
        // let input_file = "pineapple.hdt";

        let p = Path::new(input_file);
        assert!(p.exists());
        let source = std::fs::File::open(p).expect("failed to open hdt file");
        let mut hdt_reader = BufReader::new(source);

        let ci = ControlInfo::read(&mut hdt_reader).expect("failed to read HDT control info");
        let h = Header::read(&mut hdt_reader).expect("failed to read HDT Header");

        let unvalidated_dict = hdt::four_sect_dict::FourSectDict::read(&mut hdt_reader)
            .expect("failed to read dictionary");
        unvalidated_dict
            .validate()
            .expect("invalid 4 section dictionary");

        let triples = hdt::triples::TriplesBitmap::read_sect(&mut hdt_reader)
            .expect("invalid bitmap triples");
    }
}
