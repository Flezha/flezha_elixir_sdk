//! The activation as Elixir terms, and the result read back from them.

use flz_core_ffi::veneer::{shape, Refusal};
use rustler::types::binary::{Binary, NewBinary};
use rustler::types::tuple::make_tuple;
use rustler::{Atom, Encoder, Env, Error, Term};

/// A value this library could not map, worded for the error document.
#[derive(Debug)]
pub struct Fail(pub String);

impl From<Refusal> for Fail {
    fn from(r: Refusal) -> Self {
        Fail(r.into_message())
    }
}

impl From<Error> for Fail {
    fn from(e: Error) -> Self {
        Fail(format!("{e:?}"))
    }
}

pub fn bin<'a>(env: Env<'a>, bytes: &[u8]) -> Term<'a> {
    let mut b = NewBinary::new(env, bytes.len());
    b.as_mut_slice().copy_from_slice(bytes);
    let b: Binary = b.into();
    b.encode(env)
}

pub struct Sink<'a>(pub Env<'a>);

impl<'a> shape::Sink for Sink<'a> {
    type Value = Term<'a>;
    type Map = Term<'a>;
    type List = Vec<Term<'a>>;
    type Error = Fail;

    fn map(&self) -> Result<Term<'a>, Fail> {
        Ok(Term::map_new(self.0))
    }
    fn set(&self, map: &mut Term<'a>, key: shape::Key, value: Term<'a>) -> Result<(), Fail> {
        *map = map.map_put(Atom::from_str(self.0, key.snake)?, value)?;
        Ok(())
    }
    fn finish_map(&self, map: Term<'a>) -> Result<Term<'a>, Fail> {
        Ok(map)
    }
    fn list(&self, capacity: usize) -> Result<Vec<Term<'a>>, Fail> {
        Ok(Vec::with_capacity(capacity))
    }
    fn push(&self, list: &mut Vec<Term<'a>>, value: Term<'a>) -> Result<(), Fail> {
        list.push(value);
        Ok(())
    }
    fn finish_list(&self, list: Vec<Term<'a>>) -> Result<Term<'a>, Fail> {
        Ok(list.encode(self.0))
    }
    fn bytes(&self, v: Vec<u8>) -> Result<Term<'a>, Fail> {
        Ok(bin(self.0, &v))
    }
    fn text(&self, v: String) -> Result<Term<'a>, Fail> {
        Ok(v.encode(self.0))
    }
    fn word(&self, v: &'static str) -> Result<Term<'a>, Fail> {
        Ok(Atom::from_str(self.0, v)?.encode(self.0))
    }
    fn pair(&self, key: String, value: String) -> Result<Term<'a>, Fail> {
        Ok(make_tuple(
            self.0,
            &[key.encode(self.0), value.encode(self.0)],
        ))
    }
    fn u64(&self, v: u64) -> Result<Term<'a>, Fail> {
        Ok(v.encode(self.0))
    }
    fn u32(&self, v: u32) -> Result<Term<'a>, Fail> {
        Ok(v.encode(self.0))
    }
    fn bool(&self, v: bool) -> Result<Term<'a>, Fail> {
        Ok(v.encode(self.0))
    }
}

pub struct Source<'a>(pub Env<'a>);

impl<'a> shape::Source for Source<'a> {
    type Value = Term<'a>;
    type Map = Term<'a>;
    type Error = Fail;

    fn get(&self, map: &Term<'a>, key: shape::Key) -> Result<Option<Term<'a>>, Fail> {
        match map.map_get(Atom::from_str(self.0, key.snake)?) {
            Ok(v) if v.is_atom() && v.decode::<Atom>()? == rustler::types::atom::nil() => Ok(None),
            Ok(v) => Ok(Some(v)),
            Err(_) => Ok(None),
        }
    }
    fn map(&self, v: &Term<'a>, what: &str) -> Result<Term<'a>, Fail> {
        if v.is_map() {
            Ok(*v)
        } else {
            Err(Fail(format!("{what} is not a map")))
        }
    }
    fn text(&self, v: &Term<'a>) -> Result<String, Fail> {
        if v.is_atom() {
            return Ok(v.atom_to_string()?);
        }
        v.decode::<String>()
            .map_err(|_| Fail("a word or text is not an atom or a string".to_string()))
    }
    fn bytes(&self, v: &Term<'a>, what: &str) -> Result<Vec<u8>, Fail> {
        v.decode::<Binary>()
            .map(|b| b.as_slice().to_vec())
            .map_err(|_| Fail(format!("{what} is not a binary")))
    }
    fn u64(&self, v: &Term<'a>, what: &str) -> Result<u64, Fail> {
        v.decode::<u64>()
            .map_err(|_| Fail(format!("{what} is not a non-negative integer")))
    }
    fn u32(&self, v: &Term<'a>) -> Result<u32, Fail> {
        v.decode::<u32>()
            .map_err(|_| Fail("a count is not a non-negative integer".to_string()))
    }
    fn bool(&self, v: &Term<'a>) -> Result<bool, Fail> {
        v.decode::<bool>()
            .map_err(|_| Fail("a flag is not a boolean".to_string()))
    }
    fn items(&self, v: &Term<'a>) -> Result<Vec<Term<'a>>, Fail> {
        v.decode::<Vec<Term<'a>>>()
            .map_err(|_| Fail("a list is not a list".to_string()))
    }
    fn pair(&self, v: &Term<'a>) -> Result<(String, String), Fail> {
        v.decode::<(String, String)>()
            .map_err(|_| Fail("a metadata entry is not a {key, value} pair of strings".to_string()))
    }
}
